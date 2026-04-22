from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from food_data_ingestion.services.ingestion_service import IngestionResult, IngestionService


@dataclass
class FakeConnector:
    result: dict
    calls: list[tuple[str, tuple[str, ...], str]] | None = None

    def __post_init__(self):
        if self.calls is None:
            self.calls = []

    def fetch_place_detail(self, place_id: str, *, fields=None, language="zh-TW", crawl_policy=None):
        self.calls.append((place_id, tuple(fields or ()), language))
        return self.result


class FakeCrawlJobRepository:
    def __init__(self):
        self.created = []
        self.running = []
        self.success = []
        self.failed = []
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


class FakeCacheRepository:
    def __init__(self):
        self.upserts = []
        self.mark_hits = []

    def upsert(self, entry):
        self.upserts.append(entry)

    def mark_hit(self, cache_key, *, accessed_at):
        self.mark_hits.append((cache_key, accessed_at))


class FakeRawRepository:
    def __init__(self):
        self.created = []
        self.next_id = 10

    def create(self, payload):
        self.created.append(payload)
        raw_id = self.next_id
        self.next_id += 1
        return raw_id


class FakeRestaurantRepository:
    def __init__(self):
        self.calls = []
        self.next_id = 100

    def upsert(self, parsed):
        self.calls.append(parsed)
        restaurant_id = self.next_id
        self.next_id += 1
        return restaurant_id


def fake_parser(raw_document):
    return {"parsed_from": raw_document.external_id or "cached"}


def test_ingest_google_place_detail_cache_miss_writes_cache_raw_and_restaurant():
    now = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
    connector = FakeConnector(
        {
            "provider": "google_places",
            "resource_type": "place_detail",
            "cache_key": "google_places:v1:place_detail:abc",
            "normalized_url": "https://maps.googleapis.com/place/details/json?place_id=abc",
            "request_params": {"place_id": "abc", "fields": ["name"], "language": "zh-TW"},
            "status_code": 200,
            "response_headers": {"etag": "v1"},
            "response_body": {"result": {"place_id": "abc", "name": "店家"}},
            "response_text": '{"result":{"place_id":"abc","name":"店家"}}',
            "fetched_at": now,
            "expires_at": now,
            "refresh_after": None,
            "is_error": False,
            "error_message": None,
            "source_meta": {"cache_hit": False, "request_fingerprint": "f" * 64},
        }
    )
    crawl_jobs = FakeCrawlJobRepository()
    cache_repository = FakeCacheRepository()
    raw_repository = FakeRawRepository()
    restaurant_repository = FakeRestaurantRepository()
    service = IngestionService(
        connector=connector,
        crawl_job_repository=crawl_jobs,
        cache_repository=cache_repository,
        raw_repository=raw_repository,
        restaurant_repository=restaurant_repository,
        parser=fake_parser,
        now_provider=lambda: now,
    )

    result = service.ingest_google_place_detail("abc")

    assert isinstance(result, IngestionResult)
    assert result.cache_hit is False
    assert result.job_id == 1
    assert result.raw_document_id == 10
    assert result.restaurant_id == 100
    assert len(cache_repository.upserts) == 1
    assert cache_repository.mark_hits == []
    assert len(raw_repository.created) == 1
    assert len(restaurant_repository.calls) == 1
    assert crawl_jobs.success[0][2] == {
        "cache_hit": False,
        "raw_document_id": 10,
        "restaurant_id_count": 1,
        "content_count": 0,
    }


def test_ingest_google_place_detail_cache_hit_marks_hit_and_skips_raw_write():
    now = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
    connector = FakeConnector(
        {
            "provider": "google_places",
            "resource_type": "place_detail",
            "cache_key": "google_places:v1:place_detail:abc",
            "normalized_url": "https://maps.googleapis.com/place/details/json?place_id=abc",
            "request_params": {"place_id": "abc", "fields": ["name"], "language": "zh-TW"},
            "status_code": 200,
            "response_headers": {"etag": "v1"},
            "response_body": {"result": {"place_id": "abc", "name": "店家"}},
            "response_text": '{"result":{"place_id":"abc","name":"店家"}}',
            "fetched_at": now,
            "expires_at": now,
            "refresh_after": None,
            "is_error": False,
            "error_message": None,
            "source_meta": {"cache_hit": True, "request_fingerprint": "f" * 64},
        }
    )
    crawl_jobs = FakeCrawlJobRepository()
    cache_repository = FakeCacheRepository()
    raw_repository = FakeRawRepository()
    restaurant_repository = FakeRestaurantRepository()
    service = IngestionService(
        connector=connector,
        crawl_job_repository=crawl_jobs,
        cache_repository=cache_repository,
        raw_repository=raw_repository,
        restaurant_repository=restaurant_repository,
        parser=fake_parser,
        now_provider=lambda: now,
    )

    result = service.ingest_google_place_detail("abc")

    assert result.cache_hit is True
    assert result.raw_document_id is None
    assert cache_repository.upserts == []
    assert cache_repository.mark_hits == [("google_places:v1:place_detail:abc", now)]
    assert raw_repository.created == []
    assert len(restaurant_repository.calls) == 1


def test_ingest_google_place_detail_marks_job_failed_when_parser_raises():
    now = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
    connector = FakeConnector(
        {
            "provider": "google_places",
            "resource_type": "place_detail",
            "cache_key": "google_places:v1:place_detail:abc",
            "normalized_url": "https://maps.googleapis.com/place/details/json?place_id=abc",
            "request_params": {"place_id": "abc", "fields": ["name"], "language": "zh-TW"},
            "status_code": 200,
            "response_headers": {"etag": "v1"},
            "response_body": {"result": {"place_id": "abc", "name": "店家"}},
            "response_text": '{"result":{"place_id":"abc","name":"店家"}}',
            "fetched_at": now,
            "expires_at": now,
            "refresh_after": None,
            "is_error": False,
            "error_message": None,
            "source_meta": {"cache_hit": False, "request_fingerprint": "f" * 64},
        }
    )
    crawl_jobs = FakeCrawlJobRepository()
    cache_repository = FakeCacheRepository()
    raw_repository = FakeRawRepository()
    restaurant_repository = FakeRestaurantRepository()

    def parser_raises(_raw_document):
        raise ValueError("broken parser")

    service = IngestionService(
        connector=connector,
        crawl_job_repository=crawl_jobs,
        cache_repository=cache_repository,
        raw_repository=raw_repository,
        restaurant_repository=restaurant_repository,
        parser=parser_raises,
        now_provider=lambda: now,
    )

    with pytest.raises(ValueError, match="broken parser"):
        service.ingest_google_place_detail("abc")

    assert len(cache_repository.upserts) == 1
    assert len(raw_repository.created) == 1
    assert restaurant_repository.calls == []
    assert crawl_jobs.failed[0][2] == "parser_error: broken parser"
