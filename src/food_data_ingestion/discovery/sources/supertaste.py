"""Supertaste discovery adapter.

Wraps the existing SupertasteSitemapIngestion + SupertasteArticleIngestion
flows behind the unified DiscoveryAdapterProtocol.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from food_data_ingestion.connectors.supertaste import (
    SupertasteConnector,
    SupertasteLiveFetcher,
)
from food_data_ingestion.discovery.adapter import BuildContext, DiscoveryDeps
from food_data_ingestion.discovery.service import UnifiedDiscoveryIngestionService
from food_data_ingestion.discovery.sources._shared import (
    InMemoryCacheRepository,
    InMemoryCandidateRepository,
    InMemoryCrawlJobRepository,
    InMemoryRawRepository,
)
from food_data_ingestion.parser_profiles.supertaste import SupertasteDiscoveryPolicy
from food_data_ingestion.services.ingestion_context import IngestionContext
from food_data_ingestion.services.supertaste_ingestion import (
    SupertasteArticleIngestion,
    SupertasteSitemapIngestion,
)


PLATFORM = "supertaste"


_STUB_INDEX = """<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<sitemap><loc>https://stub/article_sitemap_1.xml</loc></sitemap>
</sitemapindex>
"""

_STUB_SITEMAP = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<url><loc>https://supertaste.tvbs.com.tw/pack/100001</loc><lastmod>2026-04-16T11:00:00+08:00</lastmod></url>
<url><loc>https://supertaste.tvbs.com.tw/food/100002</loc><lastmod>2026-04-16T12:00:00+08:00</lastmod></url>
<url><loc>https://supertaste.tvbs.com.tw/hot/100003</loc><lastmod>2026-04-16T13:00:00+08:00</lastmod></url>
</urlset>
"""

_STUB_ARTICLE_PACK = {
    "type": "article",
    "data": {
        "articles_id": "100001",
        "title": "stub｜5間懶人包",
        "publish": "2026/04/16",
        "broadcast_date": "2026-04-16",
        "image": "https://example/img.jpg",
        "tag": "stub,測試",
        "description": "stub",
        "cat_name": "懶人包",
        "cat_en_name": "pack",
        "share_url": "https://supertaste.tvbs.com.tw/pack/100001",
        "updated_time": "2026-04-16",
        "article_content": {
            "type": "html",
            "value": (
                '<div class="info_card_app coupon" data-store_id="111" '
                'data-store_name="店A" data-tag="美食" data-keyword="日料">'
                '<div class="store-address"><p>台北市測試路1號</p></div>'
                "</div>"
                '<div class="info_card_app coupon" data-store_id="222" '
                'data-store_name="店B" data-tag="燒肉" data-keyword="美食">'
                "</div>"
            ),
        },
    },
}

_STUB_ARTICLE_FOOD = {
    "type": "article",
    "data": {
        "articles_id": "100002",
        "title": "stub｜單店",
        "publish": "2026/04/16",
        "broadcast_date": "2026-04-16",
        "image": None,
        "tag": "stub",
        "description": "x",
        "cat_name": "美食",
        "cat_en_name": "food",
        "share_url": "https://supertaste.tvbs.com.tw/food/100002",
        "updated_time": "2026-04-16",
        "article_content": {
            "type": "html",
            "value": (
                '<div class="info_card_app coupon" data-store_id="333" '
                'data-store_name="店C" data-tag="" data-keyword="">'
                "</div>"
            ),
        },
    },
}


class StubSupertasteFetcher:
    base_url = "https://stub"

    def fetch_sitemap_index(self, url=None) -> str:
        return _STUB_INDEX

    def fetch_sitemap(self, url: str) -> str:
        return _STUB_SITEMAP

    def fetch_article(self, category: str, article_id: str) -> str:
        if category == "pack":
            return json.dumps(_STUB_ARTICLE_PACK)
        return json.dumps(_STUB_ARTICLE_FOOD)


class _FetcherAdapter:
    """Wrap a duck-typed fetcher so SupertasteConnector can use it."""

    def __init__(self, inner) -> None:
        self._inner = inner
        self.base_url = getattr(inner, "base_url", "https://supertaste.tvbs.com.tw")

    def fetch_sitemap_index(self, url=None) -> str:
        try:
            return self._inner.fetch_sitemap_index(url)
        except TypeError:
            return self._inner.fetch_sitemap_index()

    def fetch_sitemap(self, url: str) -> str:
        return self._inner.fetch_sitemap(url)

    def fetch_article(self, category: str, article_id: str) -> str:
        return self._inner.fetch_article(category, article_id)


def run_supertaste_discovery(
    *,
    fetcher,
    sitemap_index_url: str | None = None,
    limit: int = 20,
    max_sitemaps: int | None = None,
    min_lastmod: str | None = None,
    raw_repository=None,
    candidate_repository=None,
    crawl_job_repository=None,
    cache_repository=None,
    transaction_manager=None,
    source_target: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run supertaste discovery once. Same signature as the legacy job entry."""
    crawl_policy = (source_target or {}).get("crawl_policy") or {}
    effective_limit = int(crawl_policy.get("limit", limit))
    effective_min_lastmod = crawl_policy.get("min_lastmod", min_lastmod)
    effective_max_sitemaps = crawl_policy.get("max_sitemaps", max_sitemaps)
    if effective_max_sitemaps is not None:
        effective_max_sitemaps = int(effective_max_sitemaps)
    effective_index_url = (source_target or {}).get("target_value") or sitemap_index_url
    source_target_id = (source_target or {}).get("id")

    raw_repository = raw_repository or InMemoryRawRepository()
    candidate_repository = candidate_repository or InMemoryCandidateRepository()
    crawl_job_repository = crawl_job_repository or InMemoryCrawlJobRepository()
    cache_repository = cache_repository or InMemoryCacheRepository()

    connector = SupertasteConnector(
        cache_repository=cache_repository,
        fetcher=fetcher if isinstance(fetcher, SupertasteLiveFetcher) else _FetcherAdapter(fetcher),
    )
    ctx = IngestionContext(
        crawl_job_repository=crawl_job_repository,
        raw_repository=raw_repository,
        transaction_manager=transaction_manager,
    )
    unified_ingestion_service = UnifiedDiscoveryIngestionService(
        candidate_repository=candidate_repository
    )
    policy = SupertasteDiscoveryPolicy(min_lastmod=effective_min_lastmod)

    sitemap_flow = SupertasteSitemapIngestion(ctx=ctx, connector=connector, policy=policy)
    article_flow = SupertasteArticleIngestion(
        ctx=ctx,
        connector=connector,
        unified_ingestion_service=unified_ingestion_service,
    )

    sitemap_result = sitemap_flow.ingest(
        effective_index_url,
        source_target_id=source_target_id,
        max_sitemaps=effective_max_sitemaps,
    )
    eligible = list(sitemap_result.entries)[:effective_limit]

    article_summaries: list[dict[str, Any]] = []
    candidate_count = 0
    single_count = 0
    roundup_count = 0
    for entry in eligible:
        result = article_flow.ingest(entry, source_target_id=source_target_id)
        if result.article_kind == "roundup":
            roundup_count += 1
        else:
            single_count += 1
        candidate_count += result.candidate_count
        article_summaries.append(
            {
                "url": entry.url,
                "category": entry.category,
                "article_id": entry.article_id,
                "article_kind": result.article_kind,
                "raw_document_id": result.raw_document_id,
                "candidate_count": result.candidate_count,
                "persisted_candidate_ids": result.persisted_candidate_ids,
            }
        )

    return {
        "platform": PLATFORM,
        "source_target_id": source_target_id,
        "sitemap_index_url": effective_index_url,
        "sitemap_count": sitemap_result.sitemap_count,
        "entry_count": len(sitemap_result.entries),
        "limit": effective_limit,
        "processed_entry_count": len(article_summaries),
        "single_count": single_count,
        "roundup_count": roundup_count,
        "candidate_count": candidate_count,
        "articles": article_summaries,
    }


@dataclass
class SupertasteDiscoveryAdapter:
    platform: str = PLATFORM
    use_stub_fetcher: bool = False

    def run(
        self,
        *,
        source_target: dict[str, Any] | None,
        deps: DiscoveryDeps,
    ) -> dict[str, Any]:
        fetcher = StubSupertasteFetcher() if self.use_stub_fetcher else SupertasteLiveFetcher()
        return run_supertaste_discovery(
            fetcher=fetcher,
            raw_repository=deps.raw_repository,
            candidate_repository=deps.candidate_repository,
            crawl_job_repository=deps.crawl_job_repository,
            cache_repository=deps.cache_repository,
            transaction_manager=deps.transaction_manager,
            source_target=source_target,
        )


def build_supertaste_adapter(ctx: BuildContext) -> SupertasteDiscoveryAdapter:
    return SupertasteDiscoveryAdapter(use_stub_fetcher=ctx.use_stub_fetcher)
