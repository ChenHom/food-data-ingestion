"""Supertaste pipeline 各來源的 flow class。

兩個 flow，各自透過共用的 IngestionContext 擁有自己的 crawl_session：

  - SupertasteSitemapIngestion：每次 crawl 跡一次，fetch sitemap index +
    每個子層的 article sitemap，回傳合併 + 過濾後的 SupertasteSitemapEntry 清單
    （呼叫端決定要 dispatch 哪些）。
  - SupertasteArticleIngestion：以 article 為單位 — fetch JSON、寫入 raw_json、
    解析、從 info_card_app 內抽出 candidate、寫入 candidate。

與 `services/candylife_ingestion.py` 對應，讓架構保持一致。
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
    """Fetch sitemap index + 各個子層的 article sitemap，回傳所有 entry。

    只會開一個 crawl_job（job_type='sitemap_index'）；子層的每個 sitemap fetch
    都重用同一個 job，並在 index 上寫一筆 raw_document，子層的每個 sitemap 另寫一筆。
    這讓 job 數量與 discovery 輪次成正比，而不是與 sitemap fan-out 成正比。
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
            # Sitemap 檔案的編號是最舊→最新（article_sitemap_1 是最舊的歸檔，大約在 2017）。
            # 反轉順序，讓最新的 article — 也就是 info_card_app HTML 結構實際存在的那批 — 先被處理。
            # 當呼叫端傳 --limit 時，這一反轉可以大幅提高 candidate 的產出量。
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
            # 若 raw_id 是 None（cache hit）則跳過 candidate 久久保存：
            # 之前成功跨出的那次已經寫了 candidates，重複使用假的
            # raw_document_id=0 會要不到 FK。

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
