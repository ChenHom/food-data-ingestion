"""IngestionContext：跨 pipeline 共用的 orchestration primitive。

提供：
- `crawl_session(...)`：context manager，負責 lock + crawl_job lifecycle +
  transaction commit/rollback + mark_running/success/failed/skipped。
- `store_raw_from_fetch(...)`：helper，在（且僅在） FetchResult 代表新資料時
  將其轉為一筆 raw_document。

各來源的「ingestion flow」類別組合這些 primitive而不是重新實作。行為與
原本的 IngestionService.ingest_google_place_detail 迴圈 byte-compatible，
讓現有的測試仍能通過。
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Callable, Iterator, Protocol

from food_data_ingestion.models.crawl_job import CrawlJobCreate
from food_data_ingestion.models.raw_document import RawDocumentCreate


class CrawlLockedError(RuntimeError):
    """當 advisory lock 無法取得時，由 crawl_session 拋出。"""


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


class RawRepositoryProtocol(Protocol):
    def create(self, payload: RawDocumentCreate) -> int: ...


class TransactionManagerProtocol(Protocol):
    def commit(self) -> None: ...

    def rollback(self) -> None: ...


class AdvisoryLockManagerProtocol(Protocol):
    def try_acquire(self, *, platform: str, resource_type: str, identifier: str) -> bool: ...

    def release(self, *, platform: str, resource_type: str, identifier: str) -> bool: ...


@dataclass
class CrawlSession:
    """負責在 crawl_session 區塊內携帶的狀態。

    flow code 會隨著作業進度變動 `success_stats`；成功時 context 會記錄它們，
    失敗時則記錄 `failure_stats`（flow 也可以預先幫它填入任何有意義的計數器）。
    """

    job_id: int
    source_target_id: int | None
    success_stats: dict[str, Any] = field(default_factory=dict)
    failure_stats: dict[str, Any] = field(default_factory=dict)
    error_prefix: str = "parser_error"


class IngestionContext:
    def __init__(
        self,
        *,
        crawl_job_repository: CrawlJobRepositoryProtocol,
        raw_repository: RawRepositoryProtocol,
        transaction_manager: TransactionManagerProtocol | None = None,
        advisory_lock_manager: AdvisoryLockManagerProtocol | None = None,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self.crawl_job_repository = crawl_job_repository
        self.raw_repository = raw_repository
        self.transaction_manager = transaction_manager
        self.advisory_lock_manager = advisory_lock_manager
        self.now_provider = now_provider or (lambda: datetime.now(UTC))

    @contextmanager
    def crawl_session(
        self,
        *,
        platform: str,
        job_type: str,
        identifier: str,
        source_target_id: int | None = None,
        request_meta: dict[str, Any] | None = None,
    ) -> Iterator[CrawlSession]:
        now = self.now_provider()
        job_id = self.crawl_job_repository.create(
            CrawlJobCreate(
                platform=platform,
                job_type=job_type,
                source_target_id=source_target_id,
                request_meta=dict(request_meta or {}),
            )
        )
        self.crawl_job_repository.mark_running(job_id, started_at=now)
        if self.transaction_manager is not None:
            self.transaction_manager.commit()

        lock_acquired = False
        if self.advisory_lock_manager is not None:
            lock_acquired = self.advisory_lock_manager.try_acquire(
                platform=platform,
                resource_type=job_type,
                identifier=identifier,
            )
            if not lock_acquired:
                finished_at = self.now_provider()
                self.crawl_job_repository.mark_skipped(
                    job_id,
                    finished_at=finished_at,
                    error_message=f"crawl_locked: {platform}/{job_type}/{identifier}",
                    stats={"cache_hit": False, "content_count": 0, "lock_acquired": False},
                )
                if self.transaction_manager is not None:
                    self.transaction_manager.commit()
                raise CrawlLockedError(f"crawl_locked: {platform}/{job_type}/{identifier}")

        session = CrawlSession(job_id=job_id, source_target_id=source_target_id)
        try:
            yield session
            finished_at = self.now_provider()
            self.crawl_job_repository.mark_success(
                job_id,
                finished_at=finished_at,
                stats=session.success_stats or None,
            )
            if self.transaction_manager is not None:
                self.transaction_manager.commit()
        except Exception as exc:
            if self.transaction_manager is not None:
                self.transaction_manager.rollback()
            finished_at = self.now_provider()
            self.crawl_job_repository.mark_failed(
                job_id,
                finished_at=finished_at,
                error_message=f"{session.error_prefix}: {exc}",
                stats=session.failure_stats or None,
            )
            if self.transaction_manager is not None:
                self.transaction_manager.commit()
            raise
        finally:
            if lock_acquired and self.advisory_lock_manager is not None:
                self.advisory_lock_manager.release(
                    platform=platform,
                    resource_type=job_type,
                    identifier=identifier,
                )

    def store_raw_from_fetch(
        self,
        fetch_result: dict[str, Any],
        *,
        crawl_job_id: int,
        source_target_id: int | None = None,
        external_id: str | None = None,
        extra_source_meta: dict[str, Any] | None = None,
        commit: bool = True,
    ) -> int | None:
        """為一次新 fetch 寫入一筆 raw_document。遇到 cache hit 時回傳 None。"""
        if bool(fetch_result.get("source_meta", {}).get("cache_hit")):
            return None
        raw_id = self.raw_repository.create(
            RawDocumentCreate.from_fetch_result(
                fetch_result,
                crawl_job_id=crawl_job_id,
                source_target_id=source_target_id,
                external_id=external_id,
                extra_source_meta=extra_source_meta,
            )
        )
        if commit and self.transaction_manager is not None:
            self.transaction_manager.commit()
        return raw_id
