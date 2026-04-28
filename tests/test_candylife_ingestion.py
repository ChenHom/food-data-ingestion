from __future__ import annotations

from datetime import UTC, datetime

from food_data_ingestion.models.crawl_job import CrawlJobCreate
from food_data_ingestion.models.raw_document import RawDocumentCreate
from food_data_ingestion.parsers.candylife_feed import ArticleKind, CandylifeFeedEntry
from food_data_ingestion.services.candylife_ingestion import (
    CandylifeArticleIngestion,
    CandylifeFeedIngestion,
)
from food_data_ingestion.services.ingestion_context import IngestionContext


SINGLE_STORE_HTML = """
<html><head><title>255 LAB café｜台中南屯咖啡廳推薦 - 糖糖's 享食生活</title>
<meta property="article:published_time" content="2026-04-21T11:45:48+00:00" /></head>
<body><article><h1>255 LAB café｜台中南屯咖啡廳推薦</h1>
<a href="https://candylife.tw/category/taichung-food/">台中美食</a>
<p>《店家資訊》</p>
<p>店家：255 LAB café 電話：04-22512075 地址：台中市南屯區大墩十一街392號 時間：平日08:00~16:00；假日10:00~18:00</p>
</article></body></html>
"""

FEED_XML = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
<item><title>255 LAB café｜單店</title><link>https://candylife.tw/255labcafe/</link>
<pubDate>Tue, 21 Apr 2026 11:45:48 +0000</pubDate><category>台中美食</category></item>
</channel></rss>"""

NOW = datetime(2026, 4, 23, 12, 0, tzinfo=UTC)


class FakeCrawlJobRepository:
    def __init__(self):
        self.created: list[CrawlJobCreate] = []
        self.statuses: list[str] = []

    def create(self, payload):
        self.created.append(payload)
        return len(self.created)

    def mark_running(self, job_id, *, started_at, worker_name=None):
        self.statuses.append((job_id, "running"))

    def mark_success(self, job_id, *, finished_at, stats=None):
        self.statuses.append((job_id, "success", stats))

    def mark_failed(self, job_id, *, finished_at, error_message, stats=None):
        self.statuses.append((job_id, "failed", error_message))

    def mark_skipped(self, job_id, *, finished_at, error_message, stats=None):
        self.statuses.append((job_id, "skipped", error_message))


class FakeRawRepository:
    def __init__(self):
        self.payloads: list[RawDocumentCreate] = []

    def create(self, payload):
        self.payloads.append(payload)
        return len(self.payloads)


class FakeUnifiedService:
    def __init__(self):
        self.calls: list[tuple] = []

    def ingest_article_candidates(self, *, article, candidates):
        self.calls.append((article, candidates))
        return list(range(1, len(candidates) + 1))


class FakeFeedConnector:
    def __init__(self, xml: str):
        self.xml = xml
        self.fetch_calls: list[str | None] = []

    def fetch_feed(self, url=None, *, crawl_policy=None):
        self.fetch_calls.append(url)
        return {
            "provider": "candylife",
            "resource_type": "feed",
            "cache_key": "candylife:v1:feed:default",
            "normalized_url": url or "https://candylife.tw/feed/",
            "request_params": {"feed_url": url},
            "status_code": 200,
            "response_headers": None,
            "response_body": None,
            "response_text": self.xml,
            "fetched_at": NOW,
            "expires_at": NOW,
            "refresh_after": None,
            "is_error": False,
            "error_message": None,
            "source_meta": {"cache_hit": False},
        }


class FakeArticleConnector:
    def __init__(self, html: str, *, cache_hit: bool = False):
        self.html = html
        self.cache_hit = cache_hit
        self.fetch_calls: list[str] = []

    def fetch_article(self, url, *, crawl_policy=None):
        self.fetch_calls.append(url)
        return {
            "provider": "candylife",
            "resource_type": "article",
            "cache_key": f"candylife:v1:article:{url}",
            "normalized_url": url,
            "request_params": {"article_url": url},
            "status_code": 200,
            "response_headers": None,
            "response_body": None,
            "response_text": None,
            "response_html": self.html,
            "fetched_at": NOW,
            "expires_at": NOW,
            "refresh_after": None,
            "is_error": False,
            "error_message": None,
            "source_meta": {"cache_hit": self.cache_hit},
        }


def _make_ctx():
    return IngestionContext(
        crawl_job_repository=FakeCrawlJobRepository(),
        raw_repository=FakeRawRepository(),
        now_provider=lambda: NOW,
    )


def test_feed_ingestion_creates_crawl_job_and_persists_raw_and_returns_entries():
    ctx = _make_ctx()
    connector = FakeFeedConnector(FEED_XML)
    flow = CandylifeFeedIngestion(ctx=ctx, connector=connector)

    result = flow.ingest("https://candylife.tw/custom/")

    assert connector.fetch_calls == ["https://candylife.tw/custom/"]
    assert result.raw_document_id == 1
    assert result.cache_hit is False
    assert len(result.entries) == 1
    job_repo = ctx.crawl_job_repository
    assert job_repo.created[0].platform == "candylife"
    assert job_repo.created[0].job_type == "feed"
    assert any(s[1] == "success" for s in job_repo.statuses)
    raw = ctx.raw_repository.payloads[0]
    assert raw.document_type == "feed"
    assert raw.raw_text is not None


def test_article_ingestion_persists_raw_html_and_saves_candidates_for_single_store():
    ctx = _make_ctx()
    connector = FakeArticleConnector(SINGLE_STORE_HTML)
    unified = FakeUnifiedService()
    flow = CandylifeArticleIngestion(
        ctx=ctx,
        connector=connector,
        unified_ingestion_service=unified,
    )
    entry = CandylifeFeedEntry(
        title="255 LAB café｜單店",
        link="https://candylife.tw/255labcafe/",
        published_at="Tue, 21 Apr 2026 11:45:48 +0000",
        categories=("台中美食",),
        article_kind=ArticleKind.SINGLE_STORE,
    )

    result = flow.ingest(entry, source_target_id=42)

    assert connector.fetch_calls == [entry.link]
    assert result.raw_document_id == 1
    assert result.candidate_count == 1
    assert result.persisted_candidate_ids == [1]
    raw = ctx.raw_repository.payloads[0]
    assert raw.raw_html == SINGLE_STORE_HTML
    assert raw.source_meta["article_kind"] == "single_store"
    assert raw.source_target_id == 42
    assert unified.calls and unified.calls[0][1][0].candidate_name == "255 LAB café"


def test_article_ingestion_skips_candidates_for_roundup_but_still_persists_raw():
    ctx = _make_ctx()
    connector = FakeArticleConnector("<html><body><article><h1>Roundup</h1></article></body></html>")
    unified = FakeUnifiedService()
    flow = CandylifeArticleIngestion(
        ctx=ctx,
        connector=connector,
        unified_ingestion_service=unified,
    )
    entry = CandylifeFeedEntry(
        title="台中乳酪蛋糕懶人包",
        link="https://candylife.tw/cheese/",
        published_at="Mon, 20 Apr 2026 13:23:56 +0000",
        categories=("懶人包特輯",),
        article_kind=ArticleKind.ROUNDUP,
    )

    result = flow.ingest(entry)

    assert result.candidate_count == 0
    assert result.persisted_candidate_ids == []
    assert unified.calls == []
    assert ctx.raw_repository.payloads[0].raw_html.startswith("<html")
