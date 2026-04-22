from __future__ import annotations

from datetime import datetime, timezone

import pytest

from food_data_ingestion.models.crawl_job import CrawlJobCreate
from food_data_ingestion.storage.crawl_job_repository import CrawlJobRepository


class FakeSession:
    def __init__(self, *, returning_row=None):
        self.returning_row = returning_row or {"id": 0}
        self.execute_returning_calls = []
        self.execute_calls = []

    def execute_returning(self, query, params):
        self.execute_returning_calls.append((query, params))
        return self.returning_row

    def execute(self, query, params):
        self.execute_calls.append((query, params))


def test_crawl_job_create_rejects_invalid_status():
    with pytest.raises(ValueError, match="invalid crawl job status"):
        CrawlJobCreate(platform="google_places", job_type="place_detail", status="queued")


def test_create_inserts_crawl_job_and_returns_id():
    scheduled_at = datetime(2026, 4, 22, 8, 0, tzinfo=timezone.utc)
    session = FakeSession(returning_row={"id": 7})
    repository = CrawlJobRepository(session)
    payload = CrawlJobCreate(
        source_target_id=11,
        platform="google_places",
        job_type="place_detail",
        status="pending",
        scheduled_at=scheduled_at,
        worker_name="worker-a",
        request_meta={"place_id": "ChIJ123"},
        stats={"cache_hit": False},
    )

    job_id = repository.create(payload)

    assert job_id == 7
    query, params = session.execute_returning_calls[0]
    assert "INSERT INTO ingestion.crawl_jobs" in query
    assert "RETURNING id" in query
    assert params[:9] == (
        11,
        "google_places",
        "place_detail",
        "pending",
        scheduled_at,
        None,
        None,
        0,
        "worker-a",
    )
    assert params[9].obj == {"place_id": "ChIJ123"}
    assert params[10].obj == {"cache_hit": False}
    assert params[11] is None


def test_mark_running_sets_status_started_at_and_increments_attempt_count():
    started_at = datetime(2026, 4, 22, 8, 5, tzinfo=timezone.utc)
    session = FakeSession()
    repository = CrawlJobRepository(session)

    repository.mark_running(7, started_at=started_at, worker_name="worker-b")

    query, params = session.execute_calls[0]
    assert "UPDATE ingestion.crawl_jobs" in query
    assert "status = 'running'" in query
    assert "attempt_count = attempt_count + 1" in query
    assert params == (started_at, "worker-b", 7)


def test_mark_success_sets_finished_at_stats_and_clears_error_message():
    finished_at = datetime(2026, 4, 22, 8, 8, tzinfo=timezone.utc)
    session = FakeSession()
    repository = CrawlJobRepository(session)

    repository.mark_success(
        7,
        finished_at=finished_at,
        stats={"cache_hit": True, "raw_document_id": 9, "restaurant_id_count": 1, "content_count": 0},
    )

    query, params = session.execute_calls[0]
    assert "status = 'success'" in query
    assert "error_message = NULL" in query
    assert params[0] == finished_at
    assert params[1].obj == {"cache_hit": True, "raw_document_id": 9, "restaurant_id_count": 1, "content_count": 0}
    assert params[2] == 7


def test_mark_failed_sets_finished_at_error_and_stats():
    finished_at = datetime(2026, 4, 22, 8, 8, tzinfo=timezone.utc)
    session = FakeSession()
    repository = CrawlJobRepository(session)

    repository.mark_failed(
        7,
        finished_at=finished_at,
        error_message="rate limit",
        stats={"cache_hit": False, "content_count": 0},
    )

    query, params = session.execute_calls[0]
    assert "status = 'failed'" in query
    assert params[0] == finished_at
    assert params[1].obj == {"cache_hit": False, "content_count": 0}
    assert params[2] == "rate limit"
    assert params[3] == 7


def test_mark_skipped_sets_finished_at_error_and_stats():
    finished_at = datetime(2026, 4, 22, 8, 9, tzinfo=timezone.utc)
    session = FakeSession()
    repository = CrawlJobRepository(session)

    repository.mark_skipped(
        7,
        finished_at=finished_at,
        error_message="crawl_locked: google_places/place_detail/abc",
        stats={"cache_hit": False, "content_count": 0, "lock_acquired": False},
    )

    query, params = session.execute_calls[0]
    assert "status = 'skipped'" in query
    assert params[0] == finished_at
    assert params[1].obj == {"cache_hit": False, "content_count": 0, "lock_acquired": False}
    assert params[2] == "crawl_locked: google_places/place_detail/abc"
    assert params[3] == 7
