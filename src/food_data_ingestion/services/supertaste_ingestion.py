"""Per-source flow classes for the Supertaste pipeline.

Two flows, each owning their own crawl_session via the shared IngestionContext:

  - SupertasteSitemapIngestion: runs once per crawl, fetches the sitemap index
    + each child article sitemap, returns the merged + filtered list of
    SupertasteSitemapEntry (caller decides which to dispatch).
  - SupertasteArticleIngestion: per article — fetch JSON, store raw_json,
    parse, extract candidates from info_card_app, persist candidates.

Mirrors `services/candylife_ingestion.py` so the architecture stays uniform.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from food_data_ingestion.parser_profiles.supertaste import (
    SupertasteDiscoveryPolicy,
    SupertasteParserProfile,
    classify_article_kind,
)
from food_data_ingestion.parsers.supertaste import extract_supertaste_article
from food_data_ingestion.parsers.supertaste_sitemap import (
    SupertasteSitemapEntry,
    parse_supertaste_sitemap,
    parse_supertaste_sitemap_index,
)
from food_data_ingestion.services.ingestion_context import IngestionContext


class SupertasteConnectorProtocol(Protocol):
    def fetch_sitemap_index(
        self, url: str | None = None, *, crawl_policy: dict[str, Any] | None = None
    ) -> dict[str, Any]: ...

    def fetch_sitemap(
        self, url: str, *, crawl_policy: dict[str, Any] | None = None
    ) -> dict[str, Any]: ...

    def fetch_article(
        self, category: str, article_id: str, *, crawl_policy: dict[str, Any] | None = None
    ) -> dict[str, Any]: ...


class UnifiedDiscoveryIngestionProtocol(Protocol):
    def ingest_article_candidates(self, *, article, candidates) -> list[int]: ...


@dataclass(frozen=True)
class SitemapIngestionResult:
    job_id: int
    raw_document_id: int | None
    cache_hit: bool
    entries: tuple[SupertasteSitemapEntry, ...]
    sitemap_count: int


@dataclass(frozen=True)
class ArticleIngestionResult:
    job_id: int
    raw_document_id: int | None
    cache_hit: bool
    article_kind: str
    candidate_count: int
    persisted_candidate_ids: list[int] = field(default_factory=list)


class SupertasteSitemapIngestion:
    """Fetch the sitemap index + each child article sitemap, return all entries.

    Only one crawl_job is opened (job_type='sitemap_index'); per-child sitemap
    fetches reuse the same job and write one raw_document for the index plus
    one per child sitemap. This keeps job-count proportional to discovery
    rounds rather than sitemap fan-out.
    """

    PLATFORM = "supertaste"
    JOB_TYPE = "sitemap_index"

    def __init__(
        self,
        *,
        ctx: IngestionContext,
        connector: SupertasteConnectorProtocol,
        policy: SupertasteDiscoveryPolicy = SupertasteDiscoveryPolicy(),
    ) -> None:
        self.ctx = ctx
        self.connector = connector
        self.policy = policy

    def ingest(
        self,
        sitemap_index_url: str | None = None,
        *,
        source_target_id: int | None = None,
        crawl_policy: dict[str, Any] | None = None,
        max_sitemaps: int | None = None,
    ) -> SitemapIngestionResult:
        identifier = sitemap_index_url or "default"
        request_meta: dict[str, Any] = {"sitemap_index_url": sitemap_index_url}
        if source_target_id is not None:
            request_meta["source_target_id"] = source_target_id

        captured: dict[str, Any] = {
            "raw_document_id": None,
            "cache_hit": False,
            "entries": (),
            "sitemap_count": 0,
        }

        with self.ctx.crawl_session(
            platform=self.PLATFORM,
            job_type=self.JOB_TYPE,
            identifier=identifier,
            source_target_id=source_target_id,
            request_meta=request_meta,
        ) as session:
            index_fetch = self.connector.fetch_sitemap_index(
                sitemap_index_url, crawl_policy=crawl_policy
            )
            index_cache_hit = bool(index_fetch.get("source_meta", {}).get("cache_hit"))

            index_raw_id = self.ctx.store_raw_from_fetch(
                index_fetch,
                crawl_job_id=session.job_id,
                source_target_id=source_target_id,
                external_id=identifier,
            )

            session.failure_stats = {
                "cache_hit": index_cache_hit,
                "raw_document_id": index_raw_id,
                "content_count": 0,
            }

            child_urls = parse_supertaste_sitemap_index(
                index_fetch.get("response_text") or ""
            )
            # Sitemap files are numbered oldest→newest (article_sitemap_1 is the
            # oldest archive, ~2017). Reverse so newest articles—where the
            # info_card_app HTML structure actually exists—are processed first.
            # This dramatically lifts candidate yield when callers pass --limit.
            child_urls = tuple(reversed(child_urls))
            if max_sitemaps is not None:
                child_urls = child_urls[:max_sitemaps]

            all_entries: list[SupertasteSitemapEntry] = []
            for child_url in child_urls:
                child_fetch = self.connector.fetch_sitemap(
                    child_url, crawl_policy=crawl_policy
                )
                self.ctx.store_raw_from_fetch(
                    child_fetch,
                    crawl_job_id=session.job_id,
                    source_target_id=source_target_id,
                    external_id=child_url,
                )
                all_entries.extend(
                    parse_supertaste_sitemap(child_fetch.get("response_text") or "")
                )

            filtered = tuple(self.policy.filter_entries(all_entries))

            captured.update(
                raw_document_id=index_raw_id,
                cache_hit=index_cache_hit,
                entries=filtered,
                sitemap_count=len(child_urls),
            )

            session.success_stats = {
                "cache_hit": index_cache_hit,
                "raw_document_id": index_raw_id,
                "sitemap_count": len(child_urls),
                "entry_count": len(filtered),
                "content_count": 0,
            }

        return SitemapIngestionResult(
            job_id=session.job_id,
            raw_document_id=captured["raw_document_id"],
            cache_hit=captured["cache_hit"],
            entries=captured["entries"],
            sitemap_count=captured["sitemap_count"],
        )


class SupertasteArticleIngestion:
    PLATFORM = "supertaste"
    JOB_TYPE = "article"

    def __init__(
        self,
        *,
        ctx: IngestionContext,
        connector: SupertasteConnectorProtocol,
        unified_ingestion_service: UnifiedDiscoveryIngestionProtocol,
        parser_profile: SupertasteParserProfile = SupertasteParserProfile(),
    ) -> None:
        self.ctx = ctx
        self.connector = connector
        self.unified_ingestion_service = unified_ingestion_service
        self.parser_profile = parser_profile

    def ingest(
        self,
        entry: SupertasteSitemapEntry,
        *,
        source_target_id: int | None = None,
        crawl_policy: dict[str, Any] | None = None,
    ) -> ArticleIngestionResult:
        request_meta: dict[str, Any] = {
            "article_url": entry.url,
            "article_id": entry.article_id,
            "category": entry.category,
        }
        if source_target_id is not None:
            request_meta["source_target_id"] = source_target_id

        captured: dict[str, Any] = {
            "raw_document_id": None,
            "cache_hit": False,
            "article_kind": "",
            "candidate_count": 0,
            "persisted_candidate_ids": [],
        }

        with self.ctx.crawl_session(
            platform=self.PLATFORM,
            job_type=self.JOB_TYPE,
            identifier=entry.url,
            source_target_id=source_target_id,
            request_meta=request_meta,
        ) as session:
            fetch_result = self.connector.fetch_article(
                entry.category, entry.article_id, crawl_policy=crawl_policy
            )
            cache_hit = bool(fetch_result.get("source_meta", {}).get("cache_hit"))

            extra_meta = {
                "article_id": entry.article_id,
                "category": entry.category,
                "lastmod": entry.lastmod,
                "parser_profile": self.parser_profile.name,
            }
            raw_id = self.ctx.store_raw_from_fetch(
                fetch_result,
                crawl_job_id=session.job_id,
                source_target_id=source_target_id,
                external_id=entry.article_id,
                extra_source_meta=extra_meta,
            )

            session.failure_stats = {
                "cache_hit": cache_hit,
                "raw_document_id": raw_id,
                "content_count": 0,
            }

            payload = fetch_result.get("response_body") or {}
            extraction = extract_supertaste_article(payload, source_url=entry.url)
            article_kind = classify_article_kind(
                category=entry.category, title=extraction.title
            )

            article = self.parser_profile.to_discovered_article(
                extraction=extraction,
                raw_document_id=raw_id or 0,
                article_kind=article_kind,
            )
            candidates = self.parser_profile.to_discovered_candidates(
                extraction=extraction,
                raw_document_id=raw_id or 0,
                article_kind=article_kind,
            )

            persisted_candidate_ids: list[int] = []
            if candidates and raw_id is not None:
                persisted_candidate_ids = list(
                    self.unified_ingestion_service.ingest_article_candidates(
                        article=article,
                        candidates=candidates,
                    )
                )
                if self.ctx.transaction_manager is not None:
                    self.ctx.transaction_manager.commit()
            # If raw_id is None (cache hit) we skip candidate persistence: the
            # earlier successful run already wrote the candidates, and writing
            # again with a fake raw_document_id=0 would trip the FK.

            captured.update(
                raw_document_id=raw_id,
                cache_hit=cache_hit,
                article_kind=article_kind,
                candidate_count=len(candidates),
                persisted_candidate_ids=persisted_candidate_ids,
            )

            session.success_stats = {
                "cache_hit": cache_hit,
                "raw_document_id": raw_id,
                "article_kind": article_kind,
                "candidate_count": len(candidates),
                "content_count": len(candidates),
            }

        return ArticleIngestionResult(
            job_id=session.job_id,
            raw_document_id=captured["raw_document_id"],
            cache_hit=captured["cache_hit"],
            article_kind=captured["article_kind"],
            candidate_count=captured["candidate_count"],
            persisted_candidate_ids=captured["persisted_candidate_ids"],
        )
