from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol

from food_data_ingestion.db.json import as_jsonb
from food_data_ingestion.models.crawl_job import CrawlJobCreate


class SessionProtocol(Protocol):
    def execute(self, query: str, params: tuple[Any, ...]) -> None: ...

    def execute_returning(self, query: str, params: tuple[Any, ...]) -> dict[str, Any] | None: ...


class CrawlJobRepository:
    def __init__(self, session: SessionProtocol):
        self.session = session

    def create(self, payload: CrawlJobCreate) -> int:
        row = self.session.execute_returning(
            """
            INSERT INTO ingestion.crawl_jobs (
                source_target_id,
                platform,
                job_type,
                status,
                scheduled_at,
                started_at,
                finished_at,
                attempt_count,
                worker_name,
                request_meta,
                stats,
                error_message
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )
            RETURNING id
            """,
            (
                payload.source_target_id,
                payload.platform,
                payload.job_type,
                payload.status,
                payload.scheduled_at,
                payload.started_at,
                payload.finished_at,
                payload.attempt_count,
                payload.worker_name,
                as_jsonb(payload.request_meta),
                as_jsonb(payload.stats),
                payload.error_message,
            ),
        )
        if not row or "id" not in row:
            raise RuntimeError("failed to create crawl job")
        return int(row["id"])

    def mark_running(self, job_id: int, *, started_at: datetime, worker_name: str | None = None) -> None:
        self.session.execute(
            """
            UPDATE ingestion.crawl_jobs
            SET status = 'running',
                started_at = %s,
                worker_name = %s,
                attempt_count = attempt_count + 1,
                updated_at = NOW()
            WHERE id = %s
            """,
            (started_at, worker_name, job_id),
        )

    def mark_success(self, job_id: int, *, finished_at: datetime, stats: dict[str, Any] | None = None) -> None:
        self.session.execute(
            """
            UPDATE ingestion.crawl_jobs
            SET status = 'success',
                finished_at = %s,
                stats = %s,
                error_message = NULL,
                updated_at = NOW()
            WHERE id = %s
            """,
            (finished_at, as_jsonb(stats or {}), job_id),
        )

    def mark_failed(
        self,
        job_id: int,
        *,
        finished_at: datetime,
        error_message: str,
        stats: dict[str, Any] | None = None,
    ) -> None:
        self.session.execute(
            """
            UPDATE ingestion.crawl_jobs
            SET status = 'failed',
                finished_at = %s,
                stats = %s,
                error_message = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (finished_at, as_jsonb(stats or {}), error_message, job_id),
        )
