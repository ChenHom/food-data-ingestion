from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from food_data_ingestion.models.crawl_job import CrawlJobCreate
from food_data_ingestion.models.raw_document import RawDocumentCreate
from food_data_ingestion.parsers.supertaste_sitemap import SupertasteSitemapEntry
from food_data_ingestion.services.ingestion_context import IngestionContext
from food_data_ingestion.services.supertaste_ingestion import (
    SupertasteArticleIngestion,
    SupertasteSitemapIngestion,
)


FIXTURES = Path(__file__).parent / "fixtures" / "supertaste"
NOW = datetime(2026, 4, 23, 12, 0, tzinfo=UTC)


class FakeCrawlJobRepository:
    def __init__(self):
        self.created: list[CrawlJobCreate] = []
        self.statuses: list[tuple] = []

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


class FakeSitemapConnector:
    def __init__(self, *, index_xml: str, child_xml: str):
        self.index_xml = index_xml
        self.child_xml = child_xml
        self.index_calls: list = []
        self.child_calls: list[str] = []

    def fetch_sitemap_index(self, url=None, *, crawl_policy=None):
        self.index_calls.append(url)
        return _text_fetch_result("sitemap_index", url or "https://x/idx.xml", self.index_xml)

    def fetch_sitemap(self, url, *, crawl_policy=None):
        self.child_calls.append(url)
        return _text_fetch_result("sitemap", url, self.child_xml)

    def fetch_article(self, category, article_id, *, crawl_policy=None):
        raise AssertionError("not used in sitemap ingestion test")


class FakeArticleConnector:
    def __init__(self, payload: dict, *, cache_hit: bool = False):
        self.payload = payload
        self.cache_hit = cache_hit
        self.calls: list[tuple[str, str]] = []

    def fetch_sitemap_index(self, url=None, *, crawl_policy=None):
        raise AssertionError("not used in article ingestion test")

    def fetch_sitemap(self, url, *, crawl_policy=None):
        raise AssertionError("not used in article ingestion test")

    def fetch_article(self, category, article_id, *, crawl_policy=None):
        self.calls.append((category, article_id))
        return {
            "provider": "supertaste",
            "resource_type": "article",
            "cache_key": f"supertaste:v1:article:{category}/{article_id}",
            "normalized_url": f"https://x/api/article/{category}/{article_id}",
            "request_params": {"category": category, "article_id": article_id},
            "status_code": 200,
            "response_headers": None,
            "response_body": self.payload,
            "response_text": None,
            "response_html": None,
            "fetched_at": NOW,
            "expires_at": NOW,
            "refresh_after": None,
            "is_error": False,
            "error_message": None,
            "source_meta": {"cache_hit": self.cache_hit},
        }


def _text_fetch_result(resource_type: str, url: str, text: str) -> dict:
    return {
        "provider": "supertaste",
        "resource_type": resource_type,
        "cache_key": f"supertaste:v1:{resource_type}:{url}",
        "normalized_url": url,
        "request_params": {},
        "status_code": 200,
        "response_headers": None,
        "response_body": None,
        "response_text": text,
        "response_html": None,
        "fetched_at": NOW,
        "expires_at": NOW,
        "refresh_after": None,
        "is_error": False,
        "error_message": None,
        "source_meta": {"cache_hit": False},
    }


def _make_ctx():
    return IngestionContext(
        crawl_job_repository=FakeCrawlJobRepository(),
        raw_repository=FakeRawRepository(),
        now_provider=lambda: NOW,
    )


def test_sitemap_ingestion_fetches_index_then_each_child_and_filters_categories():
    index_xml = (FIXTURES / "sitemap_index.xml").read_text(encoding="utf-8")
    child_xml = (FIXTURES / "article_sitemap_sample.xml").read_text(encoding="utf-8")
    ctx = _make_ctx()
    connector = FakeSitemapConnector(index_xml=index_xml, child_xml=child_xml)

    flow = SupertasteSitemapIngestion(ctx=ctx, connector=connector)
    result = flow.ingest(max_sitemaps=2)

    # index + 2 個子層 sitemap 被 fetch
    assert connector.index_calls == [None]
    assert len(connector.child_calls) == 2
    # raw_documents = 1 index + 2 children
    assert len(ctx.raw_repository.payloads) == 3
    # 預設 policy 只保留 food/pack — 樣本裡有 1 筆 food entry
    cats = {e.category for e in result.entries}
    assert cats == {"food"}
    assert result.sitemap_count == 2
    job_repo = ctx.crawl_job_repository
    assert job_repo.created[0].job_type == "sitemap_index"
    assert any(s[1] == "success" for s in job_repo.statuses)


def test_article_ingestion_persists_raw_json_and_saves_candidates():
    payload = json.loads((FIXTURES / "article_pack_348872.json").read_text(encoding="utf-8"))
    ctx = _make_ctx()
    connector = FakeArticleConnector(payload)
    unified = FakeUnifiedService()
    flow = SupertasteArticleIngestion(
        ctx=ctx, connector=connector, unified_ingestion_service=unified
    )
    entry = SupertasteSitemapEntry(
        url="https://supertaste.tvbs.com.tw/pack/348872",
        category="pack",
        article_id="348872",
        lastmod="2026-04-16T21:00:00+08:00",
    )

    result = flow.ingest(entry, source_target_id=99)

    assert connector.calls == [("pack", "348872")]
    assert result.cache_hit is False
    assert result.article_kind == "roundup"
    assert result.candidate_count == 41
    assert result.persisted_candidate_ids == list(range(1, 42))
    raw = ctx.raw_repository.payloads[0]
    assert raw.document_type == "article"
    assert isinstance(raw.raw_json, dict)
    assert raw.raw_json["data"]["articles_id"] == "348872"
    assert raw.source_meta["category"] == "pack"
    assert raw.source_target_id == 99
    assert unified.calls
    article, candidates = unified.calls[0]
    assert article.article_type == "roundup"
    assert candidates[0].extraction_method == "info_card_app"


def test_article_ingestion_cache_hit_skips_raw_persist_but_still_runs_parser():
    payload = json.loads((FIXTURES / "article_pack_348872.json").read_text(encoding="utf-8"))
    ctx = _make_ctx()
    connector = FakeArticleConnector(payload, cache_hit=True)
    unified = FakeUnifiedService()
    flow = SupertasteArticleIngestion(
        ctx=ctx, connector=connector, unified_ingestion_service=unified
    )
    entry = SupertasteSitemapEntry(
        url="https://supertaste.tvbs.com.tw/pack/348872",
        category="pack",
        article_id="348872",
    )

    result = flow.ingest(entry)

    assert result.cache_hit is True
    assert result.raw_document_id is None
    assert ctx.raw_repository.payloads == []
    # candidate 仍然從被快取的 payload 裡被抽出並寫入
    assert result.candidate_count == 41
