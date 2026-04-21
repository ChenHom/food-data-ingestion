from __future__ import annotations

from datetime import datetime, timedelta, timezone

from food_data_ingestion.models.cache import ApiRequestCacheEntry
from food_data_ingestion.storage.cache_repository import ApiRequestCacheRepository, build_cache_key


class FakeSession:
    def __init__(self, row=None):
        self.row = row
        self.fetchone_calls = []
        self.execute_calls = []

    def fetchone(self, query, params):
        self.fetchone_calls.append((query, params))
        return self.row

    def execute(self, query, params):
        self.execute_calls.append((query, params))


def test_build_cache_key_normalizes_parts():
    cache_key = build_cache_key("google_places", "place_detail", "ChIJ 123", version="v2")

    assert cache_key == "google_places:v2:place_detail:chij-123"


def test_get_valid_returns_entry_when_not_expired():
    now = datetime(2026, 4, 20, 12, 0, tzinfo=timezone.utc)
    row = {
        "cache_key": "google_places:v1:place_detail:abc",
        "provider": "google_places",
        "resource_type": "place_detail",
        "cache_version": "v1",
        "request_params": {},
        "status_code": 200,
        "response_body": {"name": "店家"},
        "response_text": None,
        "content_hash": "hash-1",
        "fetched_at": now - timedelta(minutes=30),
        "refresh_after": now + timedelta(minutes=10),
        "expires_at": now + timedelta(minutes=30),
        "last_accessed_at": now - timedelta(minutes=5),
        "hit_count": 3,
        "is_error": False,
        "error_message": None,
        "source_meta": {},
    }
    repo = ApiRequestCacheRepository(FakeSession(row))

    entry = repo.get_valid("google_places:v1:place_detail:abc", as_of=now)

    assert isinstance(entry, ApiRequestCacheEntry)
    assert entry.cache_key == "google_places:v1:place_detail:abc"
    assert entry.response_body == {"name": "店家"}


def test_get_valid_returns_none_when_expired():
    now = datetime(2026, 4, 20, 12, 0, tzinfo=timezone.utc)
    row = {
        "cache_key": "google_places:v1:place_detail:abc",
        "provider": "google_places",
        "resource_type": "place_detail",
        "cache_version": "v1",
        "request_params": {},
        "status_code": 200,
        "response_body": {"name": "店家"},
        "response_text": None,
        "content_hash": "hash-1",
        "fetched_at": now - timedelta(hours=2),
        "refresh_after": now - timedelta(hours=1),
        "expires_at": now - timedelta(minutes=1),
        "last_accessed_at": now - timedelta(hours=1),
        "hit_count": 3,
        "is_error": False,
        "error_message": None,
        "source_meta": {},
    }
    repo = ApiRequestCacheRepository(FakeSession(row))

    entry = repo.get_valid("google_places:v1:place_detail:abc", as_of=now)

    assert entry is None


def test_mark_hit_updates_last_accessed_at_and_increments_counter():
    now = datetime(2026, 4, 20, 12, 0, tzinfo=timezone.utc)
    session = FakeSession()
    repo = ApiRequestCacheRepository(session)

    repo.mark_hit("google_places:v1:place_detail:abc", accessed_at=now)

    query, params = session.execute_calls[0]
    assert "UPDATE ingestion.api_request_cache" in query
    assert "hit_count = hit_count + 1" in query
    assert params == (now, "google_places:v1:place_detail:abc")


def test_upsert_resets_hit_count_on_conflict():
    now = datetime(2026, 4, 20, 12, 0, tzinfo=timezone.utc)
    entry = ApiRequestCacheEntry(
        cache_key="google_places:v1:place_detail:abc",
        provider="google_places",
        resource_type="place_detail",
        cache_version="v1",
        request_params={},
        status_code=200,
        response_body={"name": "店家"},
        response_text=None,
        content_hash="hash-1",
        fetched_at=now,
        refresh_after=now + timedelta(minutes=30),
        expires_at=now + timedelta(hours=6),
        last_accessed_at=now,
        hit_count=0,
        is_error=False,
        error_message=None,
        source_meta={},
    )
    session = FakeSession()
    repo = ApiRequestCacheRepository(session)

    repo.upsert(entry)

    query, _ = session.execute_calls[0]
    assert "hit_count = EXCLUDED.hit_count" in query


def test_from_row_ignores_extra_db_columns():
    now = datetime(2026, 4, 20, 12, 0, tzinfo=timezone.utc)
    row = {
        "cache_key": "google_places:v1:place_detail:abc",
        "provider": "google_places",
        "resource_type": "place_detail",
        "cache_version": "v1",
        "request_params": {},
        "status_code": 200,
        "response_body": {"name": "店家"},
        "response_text": None,
        "content_hash": "hash-1",
        "fetched_at": now - timedelta(minutes=30),
        "refresh_after": now + timedelta(minutes=10),
        "expires_at": now + timedelta(minutes=30),
        "last_accessed_at": now - timedelta(minutes=5),
        "hit_count": 3,
        "is_error": False,
        "error_message": None,
        "source_meta": {},
        # extra columns returned by DB that are not in the model
        "created_at": now - timedelta(hours=1),
        "updated_at": now - timedelta(minutes=10),
    }

    entry = ApiRequestCacheEntry.from_row(row)

    assert isinstance(entry, ApiRequestCacheEntry)
    assert entry.cache_key == "google_places:v1:place_detail:abc"


def test_upsert_writes_insert_on_conflict_statement():
    now = datetime(2026, 4, 20, 12, 0, tzinfo=timezone.utc)
    entry = ApiRequestCacheEntry(
        cache_key="google_places:v1:place_detail:abc",
        provider="google_places",
        resource_type="place_detail",
        cache_version="v1",
        request_params={"place_id": "abc"},
        status_code=200,
        response_body={"name": "店家"},
        response_text=None,
        content_hash="hash-1",
        fetched_at=now,
        refresh_after=now + timedelta(minutes=30),
        expires_at=now + timedelta(hours=6),
        last_accessed_at=now,
        hit_count=0,
        is_error=False,
        error_message=None,
        source_meta={"source": "unit-test"},
    )
    session = FakeSession()
    repo = ApiRequestCacheRepository(session)

    repo.upsert(entry)

    query, params = session.execute_calls[0]
    assert "INSERT INTO ingestion.api_request_cache" in query
    assert "ON CONFLICT (cache_key) DO UPDATE" in query
    assert params[0] == "google_places:v1:place_detail:abc"
    assert params[1] == "google_places"
    assert params[2] == "place_detail"
    assert params[10] == "hash-1"
