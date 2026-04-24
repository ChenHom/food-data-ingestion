from __future__ import annotations

from datetime import datetime, timezone

from food_data_ingestion.parsers.candylife_feed import ArticleKind, CandylifeFeedEntry
from food_data_ingestion.services.article_discovery import CandylifeArticleDiscoveryService


SINGLE_STORE_HTML = '''
<html><head><title>255 LAB café｜台中南屯咖啡廳推薦 - 糖糖's 享食生活</title><meta property="article:published_time" content="2026-04-21T11:45:48+00:00" /></head>
<body><article><h1>255 LAB café｜台中南屯咖啡廳推薦</h1><a href="https://candylife.tw/category/taichung-food/">台中美食</a><p>《店家資訊》</p><p>店家：255 LAB café 電話：04-22512075 地址：台中市南屯區大墩十一街392號 時間：平日08:00~16:00；假日10:00~18:00</p></article></body></html>
'''


class FakeFetcher:
    def __init__(self, html_by_url: dict[str, str]):
        self.html_by_url = html_by_url

    def fetch_html(self, url: str) -> str:
        return self.html_by_url[url]


class FakeRawRepository:
    def __init__(self):
        self.calls = []

    def create(self, payload):
        self.calls.append(payload)
        return 701


class FakeUnifiedDiscoveryIngestionService:
    def __init__(self):
        self.calls = []

    def ingest_article_candidates(self, *, article, candidates):
        self.calls.append((article, candidates))
        return [91][: len(candidates)]


def test_article_discovery_service_uses_profile_and_unified_ingestion():
    entry = CandylifeFeedEntry(
        title='255 LAB café｜台中南屯咖啡廳推薦',
        link='https://candylife.tw/255labcafe/',
        published_at='Tue, 21 Apr 2026 11:45:48 +0000',
        categories=('台中美食',),
        article_kind=ArticleKind.SINGLE_STORE,
    )
    service = CandylifeArticleDiscoveryService(
        fetcher=FakeFetcher({entry.link: SINGLE_STORE_HTML}),
        raw_repository=FakeRawRepository(),
        unified_ingestion_service=FakeUnifiedDiscoveryIngestionService(),
        clock=lambda: datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc),
    )

    result = service.discover_article(entry)

    assert result.raw_document_id == 701
    assert result.candidate_count == 1
    assert result.persisted_candidate_ids == [91]
