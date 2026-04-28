"""所有 discovery adapter 共用的 in-memory 替身與 DB-backed factory。

這些 in-memory 替身與 `create_db_backed_repositories` 以前是在各 source 的 job script
裡複製貼上；集中到這裡可以讓 adapter 保持精簡，並確保每個 source 都拿到相同的
裝配。
"""

from __future__ import annotations

from typing import Any

from food_data_ingestion.db.psycopg_session import PsycopgSession
from food_data_ingestion.models.cache import ApiRequestCacheEntry
from food_data_ingestion.models.crawl_job import CrawlJobCreate
from food_data_ingestion.models.raw_document import RawDocumentCreate
from food_data_ingestion.storage import (
    ApiRequestCacheRepository,
    CrawlJobRepository,
    DiscoveredPlaceCandidateRepository,
    RawDocumentRepository,
)


class InMemoryRawRepository:
    def __init__(self) -> None:
        self.rows: list[RawDocumentCreate] = []

    def create(self, payload: RawDocumentCreate) -> int:
        self.rows.append(payload)
        return len(self.rows)


class InMemoryCandidateRepository:
    def __init__(self) -> None:
        self.saved: list[tuple[Any, list[Any]]] = []

    def save_discovered_candidates(self, *, article, candidates) -> list[int]:
        self.saved.append((article, candidates))
        return list(range(1, len(candidates) + 1))


class InMemoryCrawlJobRepository:
    def __init__(self) -> None:
        self.jobs: list[dict[str, Any]] = []

    def create(self, payload: CrawlJobCreate) -> int:
        self.jobs.append({"payload": payload, "status": "queued"})
        return len(self.jobs)

    def mark_running(self, job_id: int, *, started_at, worker_name: str | None = None) -> None:
        self.jobs[job_id - 1]["status"] = "running"

    def mark_success(self, job_id: int, *, finished_at, stats=None) -> None:
        self.jobs[job_id - 1].update(status="success", stats=stats, finished_at=finished_at)

    def mark_failed(self, job_id: int, *, finished_at, error_message, stats=None) -> None:
        self.jobs[job_id - 1].update(status="failed", error=error_message, stats=stats)

    def mark_skipped(self, job_id: int, *, finished_at, error_message, stats=None) -> None:
        self.jobs[job_id - 1].update(status="skipped", error=error_message, stats=stats)


class InMemoryCacheRepository:
    def __init__(self) -> None:
        self.upserts: list[ApiRequestCacheEntry] = []

    def get_valid(self, cache_key, *, as_of):
        return None

    def mark_hit(self, cache_key, *, accessed_at) -> None:
        return None

    def upsert(self, entry: ApiRequestCacheEntry) -> None:
        self.upserts.append(entry)


def create_db_backed_repositories(connection) -> dict[str, Any]:
    """建立一組共用同一個 PsycopgSession 的 DB-backed repo。

    同一個 session 也會成為 ``transaction_manager`` 回傳，讓 IngestionContext 可以
    在建立 crawl_job 與插入 raw_document 之間 commit（否則
    raw_documents.crawl_job_id 的 FK 會無法滿足）。
    """
    session = PsycopgSession(connection)
    return {
        "session": session,
        "raw_repository": RawDocumentRepository(session),
        "candidate_repository": DiscoveredPlaceCandidateRepository(session),
        "crawl_job_repository": CrawlJobRepository(session),
        "cache_repository": ApiRequestCacheRepository(session),
    }
