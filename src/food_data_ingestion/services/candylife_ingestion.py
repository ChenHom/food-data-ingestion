"""Candylife pipeline 各來源的 flow class。

Feed 與 article 兩個 ingestion 現在都走共用的 `IngestionContext`：
  - lock + crawl_job lifecycle
  - transaction commit/rollback
  - raw_documents 久久保存
這讓 Candylife 與 Google Places 的處理方式一致。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from food_data_ingestion.parser_profiles.candylife import (
    CandylifeDiscoveryPolicy,
    CandylifeParserProfile,
)
from food_data_ingestion.parsers.candylife import extract_candylife_article
from food_data_ingestion.parsers.candylife_feed import CandylifeFeedEntry, parse_candylife_feed
from food_data_ingestion.services.ingestion_context import IngestionContext


class CandylifeConnectorProtocol(Protocol):
    def fetch_feed(self, url: str | None = None, *, crawl_policy: dict[str, Any] | None = None) -> dict[str, Any]: ...

    def fetch_article(self, url: str, *, crawl_policy: dict[str, Any] | None = None) -> dict[str, Any]: ...


class UnifiedDiscoveryIngestionProtocol(Protocol):
    def ingest_article_candidates(self, *, article, candidates) -> list[int]: ...


@dataclass(frozen=True)
class FeedIngestionResult:
    job_id: int
    raw_document_id: int | None
    cache_hit: bool
    entries: tuple[CandylifeFeedEntry, ...]


@dataclass(frozen=True)
class ArticleIngestionResult:
    job_id: int
    raw_document_id: int | None
    cache_hit: bool
    candidate_count: int
    persisted_candidate_ids: list[int] = field(default_factory=list)


class CandylifeFeedIngestion:
    PLATFORM = "candylife"
    JOB_TYPE = "feed"

    def __init__(self, *, ctx: IngestionContext, connector: CandylifeConnectorProtocol) -> None:
        self.ctx = ctx
        self.connector = connector

    def ingest(
        self,
        feed_url: str | None = None,
        *,
        source_target_id: int | None = None,
        crawl_policy: dict[str, Any] | None = None,
    ) -> FeedIngestionResult:
        identifier = feed_url or "default"
        request_meta: dict[str, Any] = {"feed_url": feed_url}
        if source_target_id is not None:
            request_meta["source_target_id"] = source_target_id

        captured: dict[str, Any] = {"raw_document_id": None, "cache_hit": False, "entries": ()}

        with self.ctx.crawl_session(
            platform=self.PLATFORM,
            job_type=self.JOB_TYPE,
            identifier=identifier,
            source_target_id=source_target_id,
            request_meta=request_meta,
        ) as session:
            fetch_result = self.connector.fetch_feed(feed_url, crawl_policy=crawl_policy)
            cache_hit = bool(fetch_result.get("source_meta", {}).get("cache_hit"))

            raw_id = self.ctx.store_raw_from_fetch(
                fetch_result,
                crawl_job_id=session.job_id,
                source_target_id=source_target_id,
                external_id=identifier,
            )

            session.failure_stats = {
                "cache_hit": cache_hit,
                "raw_document_id": raw_id,
                "content_count": 0,
            }

            entries = tuple(parse_candylife_feed(fetch_result.get("response_text") or ""))
            captured["raw_document_id"] = raw_id
            captured["cache_hit"] = cache_hit
            captured["entries"] = entries

            session.success_stats = {
                "cache_hit": cache_hit,
                "raw_document_id": raw_id,
                "feed_entry_count": len(entries),
                "content_count": 0,
            }

        return FeedIngestionResult(
            job_id=session.job_id,
            raw_document_id=captured["raw_document_id"],
            cache_hit=captured["cache_hit"],
            entries=captured["entries"],
        )


class CandylifeArticleIngestion:
    PLATFORM = "candylife"
    JOB_TYPE = "article"

    def __init__(
        self,
        *,
        ctx: IngestionContext,
        connector: CandylifeConnectorProtocol,
        unified_ingestion_service: UnifiedDiscoveryIngestionProtocol,
        policy: CandylifeDiscoveryPolicy = CandylifeDiscoveryPolicy(),
        parser_profile: CandylifeParserProfile = CandylifeParserProfile(),
    ) -> None:
        self.ctx = ctx
        self.connector = connector
        self.unified_ingestion_service = unified_ingestion_service
        self.policy = policy
        self.parser_profile = parser_profile

    def ingest(
        self,
        entry: CandylifeFeedEntry,
        *,
        source_target_id: int | None = None,
        crawl_policy: dict[str, Any] | None = None,
    ) -> ArticleIngestionResult:
        request_meta: dict[str, Any] = {
            "article_url": entry.link,
            "article_kind": entry.article_kind.value,
        }
        if source_target_id is not None:
            request_meta["source_target_id"] = source_target_id

        captured: dict[str, Any] = {
            "raw_document_id": None,
            "cache_hit": False,
            "candidate_count": 0,
            "persisted_candidate_ids": [],
        }

        with self.ctx.crawl_session(
            platform=self.PLATFORM,
            job_type=self.JOB_TYPE,
            identifier=entry.link,
            source_target_id=source_target_id,
            request_meta=request_meta,
        ) as session:
            fetch_result = self.connector.fetch_article(entry.link, crawl_policy=crawl_policy)
            cache_hit = bool(fetch_result.get("source_meta", {}).get("cache_hit"))

            extra_meta = {
                "title": entry.title,
                "published_at": entry.published_at,
                "categories": entry.categories,
                "article_kind": entry.article_kind.value,
                "parser_profile": self.parser_profile.name,
            }
            raw_id = self.ctx.store_raw_from_fetch(
                fetch_result,
                crawl_job_id=session.job_id,
                source_target_id=source_target_id,
                external_id=entry.link,
                extra_source_meta=extra_meta,
            )

            session.failure_stats = {
                "cache_hit": cache_hit,
                "raw_document_id": raw_id,
                "content_count": 0,
            }

            html = fetch_result.get("response_html") or fetch_result.get("response_text") or ""
            extraction = extract_candylife_article(html=html, source_url=entry.link)
            article = self.parser_profile.to_discovered_article(
                extraction=extraction,
                raw_document_id=raw_id or 0,
                article_kind=entry.article_kind,
            )

            persisted_candidate_ids: list[int] = []
            candidate_count = 0
            if self.policy.should_extract_candidates(entry.article_kind):
                candidates = self.parser_profile.to_discovered_candidates(
                    extraction=extraction,
                    raw_document_id=raw_id or 0,
                    article_kind=entry.article_kind,
                )
                candidate_count = len(candidates)
                if candidates and raw_id is not None:
                    persisted_candidate_ids = list(
                        self.unified_ingestion_service.ingest_article_candidates(
                            article=article,
                            candidates=candidates,
                        )
                    )
                if self.ctx.transaction_manager is not None:
                    self.ctx.transaction_manager.commit()
            # Cache hit（raw_id 是 None）：跳過 candidate 久久保存，
            # 之前成功跨出的那次已經寫入，重複使用
            # raw_document_id=0 會要不到 FK。

            captured.update(
                raw_document_id=raw_id,
                cache_hit=cache_hit,
                candidate_count=candidate_count,
                persisted_candidate_ids=persisted_candidate_ids,
            )

            session.success_stats = {
                "cache_hit": cache_hit,
                "raw_document_id": raw_id,
                "candidate_count": candidate_count,
                "content_count": candidate_count,
            }

        return ArticleIngestionResult(
            job_id=session.job_id,
            raw_document_id=captured["raw_document_id"],
            cache_hit=captured["cache_hit"],
            candidate_count=captured["candidate_count"],
            persisted_candidate_ids=captured["persisted_candidate_ids"],
        )
