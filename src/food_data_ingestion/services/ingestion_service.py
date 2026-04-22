from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Callable, Protocol

from food_data_ingestion.models.cache import ApiRequestCacheEntry
from food_data_ingestion.models.crawl_job import CrawlJobCreate
from food_data_ingestion.models.raw_document import RawDocumentCreate


@dataclass(frozen=True)
class IngestionResult:
    cache_hit: bool
    job_id: int
    raw_document_id: int | None
    restaurant_id: int | None


class ConnectorProtocol(Protocol):
    def fetch_place_detail(
        self,
        place_id: str,
        *,
        fields: list[str] | None = None,
        language: str = "zh-TW",
        crawl_policy: dict[str, Any] | None = None,
    ) -> dict[str, Any]: ...


class CrawlJobRepositoryProtocol(Protocol):
    def create(self, payload: CrawlJobCreate) -> int: ...

    def mark_running(self, job_id: int, *, started_at: datetime, worker_name: str | None = None) -> None: ...

    def mark_success(self, job_id: int, *, finished_at: datetime, stats: dict[str, Any] | None = None) -> None: ...

    def mark_failed(
        self,
        job_id: int,
        *,
        finished_at: datetime,
        error_message: str,
        stats: dict[str, Any] | None = None,
    ) -> None: ...

    def mark_skipped(
        self,
        job_id: int,
        *,
        finished_at: datetime,
        error_message: str,
        stats: dict[str, Any] | None = None,
    ) -> None: ...


class CacheRepositoryProtocol(Protocol):
    def upsert(self, entry: ApiRequestCacheEntry) -> None: ...

    def mark_hit(self, cache_key: str, *, accessed_at: datetime) -> None: ...


class RawRepositoryProtocol(Protocol):
    def create(self, payload: RawDocumentCreate) -> int: ...


class RestaurantRepositoryProtocol(Protocol):
    def upsert(self, parsed: Any) -> int: ...


class TransactionManagerProtocol(Protocol):
    def commit(self) -> None: ...

    def rollback(self) -> None: ...


class SourceTargetRepositoryProtocol(Protocol):
    def get_crawl_policy(self, source_target_id: int) -> dict[str, Any]: ...


class AdvisoryLockManagerProtocol(Protocol):
    def try_acquire(self, *, platform: str, resource_type: str, identifier: str) -> bool: ...

    def release(self, *, platform: str, resource_type: str, identifier: str) -> bool: ...


class IngestionService:
    def __init__(
        self,
        *,
        connector: ConnectorProtocol,
        crawl_job_repository: CrawlJobRepositoryProtocol,
        cache_repository: CacheRepositoryProtocol,
        raw_repository: RawRepositoryProtocol,
        restaurant_repository: RestaurantRepositoryProtocol,
        parser: Callable[[RawDocumentCreate], Any],
        now_provider: Callable[[], datetime] | None = None,
        transaction_manager: TransactionManagerProtocol | None = None,
        source_target_repository: SourceTargetRepositoryProtocol | None = None,
        advisory_lock_manager: AdvisoryLockManagerProtocol | None = None,
    ) -> None:
        self.connector = connector
        self.crawl_job_repository = crawl_job_repository
        self.cache_repository = cache_repository
        self.raw_repository = raw_repository
        self.restaurant_repository = restaurant_repository
        self.parser = parser
        self.now_provider = now_provider or (lambda: datetime.now(UTC))
        self.transaction_manager = transaction_manager
        self.source_target_repository = source_target_repository
        self.advisory_lock_manager = advisory_lock_manager

    def ingest_google_place_detail(
        self,
        place_id: str,
        *,
        source_target_id: int | None = None,
        crawl_policy: dict[str, Any] | None = None,
    ) -> IngestionResult:
        now = self.now_provider()
        effective_crawl_policy = self._resolve_crawl_policy(
            source_target_id=source_target_id,
            crawl_policy=crawl_policy,
        )
        job_id = self.crawl_job_repository.create(
            CrawlJobCreate(
                platform="google_places",
                job_type="place_detail",
                source_target_id=source_target_id,
                request_meta={
                    "place_id": place_id,
                    **({"source_target_id": source_target_id} if source_target_id is not None else {}),
                },
            )
        )
        self.crawl_job_repository.mark_running(job_id, started_at=now)
        if self.transaction_manager is not None:
            self.transaction_manager.commit()

        cache_hit = False
        raw_document_id: int | None = None
        lock_acquired = False

        if self.advisory_lock_manager is not None:
            lock_acquired = self.advisory_lock_manager.try_acquire(
                platform="google_places",
                resource_type="place_detail",
                identifier=place_id,
            )
            if not lock_acquired:
                finished_at = self.now_provider()
                self.crawl_job_repository.mark_skipped(
                    job_id,
                    finished_at=finished_at,
                    error_message=f"crawl_locked: google_places/place_detail/{place_id}",
                    stats={
                        "cache_hit": False,
                        "content_count": 0,
                        "lock_acquired": False,
                    },
                )
                if self.transaction_manager is not None:
                    self.transaction_manager.commit()
                raise RuntimeError(f"crawl_locked: google_places/place_detail/{place_id}")

        try:
            fetch_result = self.connector.fetch_place_detail(place_id, crawl_policy=effective_crawl_policy)
            cache_hit = bool(fetch_result.get("source_meta", {}).get("cache_hit"))

            if cache_hit:
                self.cache_repository.mark_hit(fetch_result["cache_key"], accessed_at=now)
            else:
                cache_entry = ApiRequestCacheEntry(
                    cache_key=fetch_result["cache_key"],
                    provider=fetch_result["provider"],
                    resource_type=fetch_result["resource_type"],
                    request_fingerprint=fetch_result.get("source_meta", {}).get("request_fingerprint"),
                    request_params=fetch_result.get("request_params") or {},
                    normalized_url=fetch_result.get("normalized_url"),
                    status_code=fetch_result.get("status_code"),
                    response_headers=fetch_result.get("response_headers"),
                    response_body=fetch_result.get("response_body"),
                    response_text=fetch_result.get("response_text"),
                    fetched_at=fetch_result["fetched_at"],
                    refresh_after=fetch_result.get("refresh_after"),
                    expires_at=fetch_result["expires_at"],
                    last_accessed_at=fetch_result["fetched_at"],
                    is_error=fetch_result.get("is_error", False),
                    error_message=fetch_result.get("error_message"),
                    source_meta=fetch_result.get("source_meta") or {},
                )
                self.cache_repository.upsert(cache_entry)
                raw_document_id = self.raw_repository.create(
                    RawDocumentCreate(
                        crawl_job_id=job_id,
                        source_target_id=source_target_id,
                        platform=fetch_result["provider"],
                        document_type=fetch_result["resource_type"],
                        source_url=fetch_result.get("normalized_url"),
                        external_id=place_id,
                        http_status=fetch_result.get("status_code"),
                        fetched_at=fetch_result["fetched_at"],
                        raw_json=fetch_result.get("response_body") if isinstance(fetch_result.get("response_body"), dict | list) else None,
                        raw_text=fetch_result.get("response_text"),
                        response_headers=fetch_result.get("response_headers"),
                        source_meta=fetch_result.get("source_meta") or {},
                    )
                )
                if self.transaction_manager is not None:
                    self.transaction_manager.commit()

            parser_input = RawDocumentCreate(
                crawl_job_id=job_id,
                source_target_id=source_target_id,
                platform=fetch_result["provider"],
                document_type=fetch_result["resource_type"],
                source_url=fetch_result.get("normalized_url"),
                external_id=place_id,
                http_status=fetch_result.get("status_code"),
                fetched_at=fetch_result["fetched_at"],
                raw_json=fetch_result.get("response_body") if isinstance(fetch_result.get("response_body"), dict | list) else None,
                raw_text=fetch_result.get("response_text"),
                response_headers=fetch_result.get("response_headers"),
                source_meta=fetch_result.get("source_meta") or {},
            )
            parsed = self.parser(parser_input)
            restaurant_id = self.restaurant_repository.upsert(parsed)
            if self.transaction_manager is not None:
                self.transaction_manager.commit()
            finished_at = self.now_provider()
            self.crawl_job_repository.mark_success(
                job_id,
                finished_at=finished_at,
                stats={
                    "cache_hit": cache_hit,
                    "raw_document_id": raw_document_id,
                    "restaurant_id_count": 1,
                    "content_count": 0,
                    **({"source_target_id": source_target_id} if source_target_id is not None else {}),
                },
            )
            if self.transaction_manager is not None:
                self.transaction_manager.commit()
            return IngestionResult(
                cache_hit=cache_hit,
                job_id=job_id,
                raw_document_id=raw_document_id,
                restaurant_id=restaurant_id,
            )
        except Exception as exc:
            if self.transaction_manager is not None:
                self.transaction_manager.rollback()
            finished_at = self.now_provider()
            self.crawl_job_repository.mark_failed(
                job_id,
                finished_at=finished_at,
                error_message=f"parser_error: {exc}",
                stats={
                    "cache_hit": cache_hit,
                    "raw_document_id": raw_document_id,
                    "content_count": 0,
                    **({"source_target_id": source_target_id} if source_target_id is not None else {}),
                },
            )
            if self.transaction_manager is not None:
                self.transaction_manager.commit()
            raise
        finally:
            if lock_acquired and self.advisory_lock_manager is not None:
                self.advisory_lock_manager.release(
                    platform="google_places",
                    resource_type="place_detail",
                    identifier=place_id,
                )

    def _resolve_crawl_policy(
        self,
        *,
        source_target_id: int | None,
        crawl_policy: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        resolved: dict[str, Any] = {}
        if source_target_id is not None and self.source_target_repository is not None:
            resolved.update(self.source_target_repository.get_crawl_policy(source_target_id))
        if crawl_policy:
            resolved.update(crawl_policy)
        return resolved or None
