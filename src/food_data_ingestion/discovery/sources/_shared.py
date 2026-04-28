"""Shared in-memory fakes + DB-backed factory used by every discovery adapter.

Both fakes and `create_db_backed_repositories` were previously copy-pasted
across the per-source job scripts; centralising them here keeps adapters
small and ensures every source gets the same wiring.
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
    """Build a full set of DB-backed repos sharing one PsycopgSession.

    The session is also returned as ``transaction_manager`` so IngestionContext
    can commit between crawl_job creation and raw_document inserts (otherwise
    the FK from raw_documents.crawl_job_id is unsatisfiable).
    """
    session = PsycopgSession(connection)
    return {
        "session": session,
        "raw_repository": RawDocumentRepository(session),
        "candidate_repository": DiscoveredPlaceCandidateRepository(session),
        "crawl_job_repository": CrawlJobRepository(session),
        "cache_repository": ApiRequestCacheRepository(session),
    }
