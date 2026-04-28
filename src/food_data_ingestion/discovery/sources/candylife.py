"""Candylife discovery adapter。

包裝現有的 CandylifeFeedIngestion + CandylifeArticleIngestion 流程，
讓 runner 可以用統一的方式對待 candylife 與其他來源。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from food_data_ingestion.connectors.candylife import CandylifeConnector, CandylifeLiveFetcher
from food_data_ingestion.discovery.adapter import BuildContext, DiscoveryDeps
from food_data_ingestion.discovery.service import UnifiedDiscoveryIngestionService
from food_data_ingestion.discovery.sources._shared import (
    InMemoryCacheRepository,
    InMemoryCandidateRepository,
    InMemoryCrawlJobRepository,
    InMemoryRawRepository,
)
from food_data_ingestion.parser_profiles.candylife import CandylifeDiscoveryPolicy
from food_data_ingestion.parsers.candylife_feed import ArticleKind
from food_data_ingestion.services.candylife_ingestion import (
    CandylifeArticleIngestion,
    CandylifeFeedIngestion,
)
from food_data_ingestion.services.ingestion_context import IngestionContext


PLATFORM = "candylife"


class StubCandylifeFetcher:
    """可直接注入 CandylifeConnector。"""

    def fetch_feed(self, url: str | None = None) -> str:
        return '''<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0"><channel>
          <item><title>阿發現炒｜單店</title><link>https://candylife.tw/a/</link><pubDate>Thu, 23 Apr 2026 06:24:32 +0000</pubDate><category>台中美食</category></item>
          <item><title>台中乳酪蛋糕懶人包｜四間</title><link>https://candylife.tw/b/</link><pubDate>Mon, 20 Apr 2026 13:23:56 +0000</pubDate><category>懶人包特輯</category></item>
        </channel></rss>'''

    def fetch_html(self, url: str) -> str:
        if url.endswith('/a/'):
            return '<html><head><title>阿發現炒｜單店 - 糖糖\'s 享食生活</title></head><body><article><h1>阿發現炒｜單店</h1><p>《店家資訊》</p><p>店家：阿發現炒 電話：04-12345678 地址：台中市中區測試路1號 時間：10:00~18:00</p></article></body></html>'
        return '<html><head><title>台中乳酪蛋糕懶人包 - 糖糖\'s 享食生活</title></head><body><article><h1>台中乳酪蛋糕懶人包</h1></article></body></html>'


def run_candylife_discovery(
    *,
    fetcher,
    min_year: int = 2025,
    limit: int = 20,
    raw_repository=None,
    candidate_repository=None,
    crawl_job_repository=None,
    cache_repository=None,
    transaction_manager=None,
    source_target: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """執行一次 candylife discovery"""
    crawl_policy = (source_target or {}).get('crawl_policy') or {}
    effective_min_year = int(crawl_policy.get('min_year', min_year))
    effective_limit = int(crawl_policy.get('limit', limit))
    feed_url = (source_target or {}).get('target_value')
    source_target_id = (source_target or {}).get('id')

    raw_repository = raw_repository or InMemoryRawRepository()
    candidate_repository = candidate_repository or InMemoryCandidateRepository()
    crawl_job_repository = crawl_job_repository or InMemoryCrawlJobRepository()
    cache_repository = cache_repository or InMemoryCacheRepository()

    connector = CandylifeConnector(
        cache_repository=cache_repository,
        fetcher=fetcher,
    )
    ctx = IngestionContext(
        crawl_job_repository=crawl_job_repository,
        raw_repository=raw_repository,
        transaction_manager=transaction_manager,
    )
    unified_ingestion_service = UnifiedDiscoveryIngestionService(candidate_repository=candidate_repository)
    policy = CandylifeDiscoveryPolicy(min_year=effective_min_year)

    feed_flow = CandylifeFeedIngestion(ctx=ctx, connector=connector)
    article_flow = CandylifeArticleIngestion(
        ctx=ctx,
        connector=connector,
        unified_ingestion_service=unified_ingestion_service,
        policy=policy,
    )

    feed_result = feed_flow.ingest(feed_url, source_target_id=source_target_id)
    eligible_entries = [entry for entry in feed_result.entries if policy.should_process_entry(entry)][:effective_limit]

    article_summaries: list[dict[str, Any]] = []
    candidate_count = 0
    single_store_count = 0
    roundup_count = 0
    for entry in eligible_entries:
        result = article_flow.ingest(entry, source_target_id=source_target_id)
        if entry.article_kind is ArticleKind.SINGLE_STORE:
            single_store_count += 1
        else:
            roundup_count += 1
        candidate_count += result.candidate_count
        article_summaries.append(
            {
                'title': entry.title,
                'link': entry.link,
                'published_at': entry.published_at,
                'article_kind': entry.article_kind.value,
                'raw_document_id': result.raw_document_id,
                'candidate_count': result.candidate_count,
                'persisted_candidate_ids': result.persisted_candidate_ids,
            }
        )

    return {
        'platform': PLATFORM,
        'source_target_id': source_target_id,
        'min_year': effective_min_year,
        'limit': effective_limit,
        'feed_entry_count': len(feed_result.entries),
        'eligible_entry_count': len(eligible_entries),
        'processed_entry_count': len(article_summaries),
        'single_store_count': single_store_count,
        'roundup_count': roundup_count,
        'candidate_count': candidate_count,
        'articles': article_summaries,
    }


@dataclass
class CandylifeDiscoveryAdapter:
    platform: str = PLATFORM
    use_stub_fetcher: bool = False

    def run(
        self,
        *,
        source_target: dict[str, Any] | None,
        deps: DiscoveryDeps,
    ) -> dict[str, Any]:
        fetcher = StubCandylifeFetcher() if self.use_stub_fetcher else CandylifeLiveFetcher()
        return run_candylife_discovery(
            fetcher=fetcher,
            raw_repository=deps.raw_repository,
            candidate_repository=deps.candidate_repository,
            crawl_job_repository=deps.crawl_job_repository,
            cache_repository=deps.cache_repository,
            transaction_manager=deps.transaction_manager,
            source_target=source_target,
        )


def build_candylife_adapter(ctx: BuildContext) -> CandylifeDiscoveryAdapter:
    return CandylifeDiscoveryAdapter(use_stub_fetcher=ctx.use_stub_fetcher)
