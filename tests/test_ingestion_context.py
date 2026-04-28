from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from food_data_ingestion.services.ingestion_context import (
    CrawlLockedError,
    IngestionContext,
)


@dataclass
class FakeCrawlJobRepository:
    def __init__(self):
        self.created = []
        self.running = []
        self.success = []
        self.failed = []
        self.skipped = []
        self.next_id = 1

    def create(self, payload):
        self.created.append(payload)
        job_id = self.next_id
        self.next_id += 1
        return job_id

    def mark_running(self, job_id, *, started_at, worker_name=None):
        self.running.append((job_id, started_at, worker_name))

    def mark_success(self, job_id, *, finished_at, stats=None):
        self.success.append((job_id, finished_at, stats))

    def mark_failed(self, job_id, *, finished_at, error_message, stats=None):
        self.failed.append((job_id, finished_at, error_message, stats))

    def mark_skipped(self, job_id, *, finished_at, error_message, stats=None):
        self.skipped.append((job_id, finished_at, error_message, stats))


class FakeRawRepository:
    def __init__(self):
        self.created = []
        self.next_id = 10

    def create(self, payload):
        self.created.append(payload)
        raw_id = self.next_id
        self.next_id += 1
        return raw_id


class FakeTransactionManager:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class FakeLockManager:
    def __init__(self, *, acquire=True):
        self.acquire = acquire
        self.try_calls = []
        self.release_calls = []

    def try_acquire(self, *, platform, resource_type, identifier):
        self.try_calls.append((platform, resource_type, identifier))
        return self.acquire

    def release(self, *, platform, resource_type, identifier):
        self.release_calls.append((platform, resource_type, identifier))
        return True


NOW = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)


def _make_ctx(*, lock=None, txn=None):
    return IngestionContext(
        crawl_job_repository=FakeCrawlJobRepository(),
        raw_repository=FakeRawRepository(),
        transaction_manager=txn,
        advisory_lock_manager=lock,
        now_provider=lambda: NOW,
    )


def test_crawl_session_marks_success_with_stats_and_releases_lock():
    lock = FakeLockManager(acquire=True)
    txn = FakeTransactionManager()
    ctx = _make_ctx(lock=lock, txn=txn)

    with ctx.crawl_session(
        platform="google_places",
        job_type="place_detail",
        identifier="abc",
        source_target_id=42,
        request_meta={"place_id": "abc"},
    ) as session:
        session.success_stats = {"cache_hit": False, "raw_document_id": 10}

    repo = ctx.crawl_job_repository
    assert len(repo.created) == 1
    assert repo.created[0].platform == "google_places"
    assert repo.success[0][0] == session.job_id
    assert repo.success[0][2] == {"cache_hit": False, "raw_document_id": 10}
    assert repo.failed == [] and repo.skipped == []
    assert lock.try_calls == [("google_places", "place_detail", "abc")]
    assert lock.release_calls == [("google_places", "place_detail", "abc")]
    assert txn.commits >= 2 and txn.rollbacks == 0


def test_crawl_session_marks_failed_and_rolls_back_on_exception():
    lock = FakeLockManager(acquire=True)
    txn = FakeTransactionManager()
    ctx = _make_ctx(lock=lock, txn=txn)

    with pytest.raises(ValueError, match="boom"):
        with ctx.crawl_session(
            platform="google_places",
            job_type="place_detail",
            identifier="abc",
        ) as session:
            session.failure_stats = {"cache_hit": False, "raw_document_id": None, "content_count": 0}
            raise ValueError("boom")

    repo = ctx.crawl_job_repository
    assert repo.success == []
    assert repo.failed[0][2] == "parser_error: boom"
    assert repo.failed[0][3] == {"cache_hit": False, "raw_document_id": None, "content_count": 0}
    assert txn.rollbacks == 1
    assert lock.release_calls == [("google_places", "place_detail", "abc")]


def test_crawl_session_skips_when_lock_not_acquired():
    lock = FakeLockManager(acquire=False)
    ctx = _make_ctx(lock=lock)

    with pytest.raises(CrawlLockedError, match="crawl_locked"):
        with ctx.crawl_session(
            platform="google_places",
            job_type="place_detail",
            identifier="abc",
        ):
            pytest.fail("body should not execute")

    repo = ctx.crawl_job_repository
    assert repo.skipped[0][2] == "crawl_locked: google_places/place_detail/abc"
    assert repo.skipped[0][3] == {"cache_hit": False, "content_count": 0, "lock_acquired": False}
    assert repo.success == [] and repo.failed == []
    assert lock.release_calls == []


def test_store_raw_from_fetch_returns_none_on_cache_hit():
    ctx = _make_ctx()
    fetch_result = {
        "provider": "google_places",
        "resource_type": "place_detail",
        "source_meta": {"cache_hit": True},
        "response_body": {"x": 1},
        "fetched_at": NOW,
    }

    raw_id = ctx.store_raw_from_fetch(fetch_result, crawl_job_id=1, external_id="abc")

    assert raw_id is None
    assert ctx.raw_repository.created == []


def test_store_raw_from_fetch_creates_raw_document_on_miss():
    txn = FakeTransactionManager()
    ctx = _make_ctx(txn=txn)
    fetch_result = {
        "provider": "google_places",
        "resource_type": "place_detail",
        "source_meta": {"cache_hit": False, "request_fingerprint": "f" * 64},
        "response_body": {"x": 1},
        "response_text": '{"x":1}',
        "fetched_at": NOW,
        "normalized_url": "https://example/x",
        "status_code": 200,
        "response_headers": {"etag": "v1"},
    }

    raw_id = ctx.store_raw_from_fetch(
        fetch_result,
        crawl_job_id=7,
        source_target_id=99,
        external_id="abc",
    )

    assert raw_id == 10
    created = ctx.raw_repository.created[0]
    assert created.crawl_job_id == 7
    assert created.source_target_id == 99
    assert created.external_id == "abc"
    assert created.platform == "google_places"
    assert created.raw_json == {"x": 1}
    assert txn.commits == 1
