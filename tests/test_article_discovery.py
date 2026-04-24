from __future__ import annotations

from datetime import datetime, timezone

from food_data_ingestion.parsers.candylife_feed import ArticleKind, CandylifeFeedEntry
from food_data_ingestion.services.article_discovery import CandylifeArticleDiscoveryService


SINGLE_STORE_HTML = """
<html>
  <head>
    <title>255 LAB café｜台中南屯咖啡廳推薦，吸睛試管咖啡，鄰近IKEA的實驗室風格下午茶 - 糖糖's 享食生活</title>
    <meta property="article:published_time" content="2026-04-21T11:45:48+00:00" />
  </head>
  <body>
    <article>
      <h1>255 LAB café｜台中南屯咖啡廳推薦，吸睛試管咖啡，鄰近IKEA的實驗室風格下午茶</h1>
      <a href="https://candylife.tw/category/taichung-food/">台中美食</a>
      <a href="https://candylife.tw/category/taichung-food/taichung-cafe/">台中咖啡</a>
      <p>《店家資訊》</p>
      <p>店家：255 LAB café | 二五五咖啡實驗所 南屯 電話：04-22512075 地址：台中市南屯區大墩十一街392號 時間：平日08:00~16:00；假日10:00~18:00</p>
    </article>
  </body>
</html>
"""


class FakeFetcher:
    def __init__(self, html_by_url: dict[str, str]):
        self.html_by_url = html_by_url
        self.calls = []

    def fetch_html(self, url: str) -> str:
        self.calls.append(url)
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
        return [77][: len(candidates)]


def test_discover_article_ingests_single_store_article_and_saves_candidates():
    entry = CandylifeFeedEntry(
        title="255 LAB café｜台中南屯咖啡廳推薦，吸睛試管咖啡，鄰近IKEA的實驗室風格下午茶",
        link="https://candylife.tw/255labcafe/",
        published_at="Tue, 21 Apr 2026 11:45:48 +0000",
        categories=("台中美食", "台中咖啡"),
        article_kind=ArticleKind.SINGLE_STORE,
    )
    fetcher = FakeFetcher({entry.link: SINGLE_STORE_HTML})
    raw_repository = FakeRawRepository()
    unified_ingestion_service = FakeUnifiedDiscoveryIngestionService()
    service = CandylifeArticleDiscoveryService(
        fetcher=fetcher,
        raw_repository=raw_repository,
        unified_ingestion_service=unified_ingestion_service,
        clock=lambda: datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc),
    )

    result = service.discover_article(entry)

    assert result.raw_document_id == 701
    assert result.candidate_count == 1
    assert result.persisted_candidate_ids == [77]
    assert fetcher.calls == [entry.link]
    assert raw_repository.calls[0].platform == "candylife"
    assert raw_repository.calls[0].document_type == "article"
    assert unified_ingestion_service.calls[0][0].source_url == entry.link
    assert unified_ingestion_service.calls[0][1][0].candidate_name == "255 LAB café"


def test_discover_article_skips_roundup_candidate_persistence_but_still_lands_raw():
    entry = CandylifeFeedEntry(
        title="台中乳酪蛋糕懶人包｜四間在地人激推最強乳酪蛋糕，乳酪控必收藏！",
        link="https://candylife.tw/bakedcheesecakebag/",
        published_at="Mon, 20 Apr 2026 13:23:56 +0000",
        categories=("懶人包特輯", "台中美食"),
        article_kind=ArticleKind.ROUNDUP,
    )
    fetcher = FakeFetcher({entry.link: '<html><body><article><h1>台中乳酪蛋糕懶人包</h1></article></body></html>'})
    raw_repository = FakeRawRepository()
    unified_ingestion_service = FakeUnifiedDiscoveryIngestionService()
    service = CandylifeArticleDiscoveryService(
        fetcher=fetcher,
        raw_repository=raw_repository,
        unified_ingestion_service=unified_ingestion_service,
        clock=lambda: datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc),
    )

    result = service.discover_article(entry)

    assert result.raw_document_id == 701
    assert result.candidate_count == 0
    assert result.persisted_candidate_ids == []
    assert raw_repository.calls[0].source_url == entry.link
    assert unified_ingestion_service.calls == []
