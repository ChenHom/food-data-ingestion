from __future__ import annotations

from datetime import datetime, timedelta, timezone

from food_data_ingestion.config import Settings
from food_data_ingestion.connectors.google_places import (
    ESSENTIALS_DETAIL_FIELDS,
    ESSENTIALS_SEARCH_FIELDS,
    GooglePlacesConnector,
    build_request_fingerprint,
    get_google_places_ttl_policy,
)
from food_data_ingestion.models.cache import ApiRequestCacheEntry


class FakeCacheRepository:
    def __init__(self, entry: ApiRequestCacheEntry | None = None):
        self.entry = entry
        self.calls = []
        self.upserts = []
        self.mark_hits = []

    def get_valid(self, cache_key: str, *, as_of: datetime):
        self.calls.append((cache_key, as_of))
        return self.entry

    def upsert(self, entry: ApiRequestCacheEntry) -> None:
        self.upserts.append(entry)

    def mark_hit(self, cache_key: str, *, accessed_at: datetime) -> None:
        self.mark_hits.append((cache_key, accessed_at))


class FakeGooglePlacesClient:
    def __init__(self, *, detail_response=None, search_response=None, error=None):
        self.detail_response = detail_response
        self.search_response = search_response
        self.error = error
        self.detail_calls = []
        self.search_calls = []

    def fetch_place_detail(self, *, place_id, field_mask, language_code):
        self.detail_calls.append((place_id, field_mask, language_code))
        if self.error is not None:
            raise self.error
        return self.detail_response

    def search_text(self, *, text_query, field_mask, language_code, region_code):
        self.search_calls.append((text_query, field_mask, language_code, region_code))
        if self.error is not None:
            raise self.error
        return self.search_response


def _connector(*, cache, client, now):
    return GooglePlacesConnector(
        settings=Settings(google_places_api_key="test-key"),
        cache_repository=cache,
        client=client,
        now_provider=lambda: now,
    )


def test_build_request_fingerprint_is_stable_for_equivalent_params():
    a = build_request_fingerprint(
        provider="google_places",
        resource_type="place_detail",
        request_params={"place_id": "abc", "field_mask": ["id", "displayName"], "language_code": "zh-TW"},
        normalized_url="https://places.googleapis.com/v1/places/abc",
    )
    b = build_request_fingerprint(
        provider="google_places",
        resource_type="place_detail",
        request_params={"language_code": "zh-TW", "field_mask": ["id", "displayName"], "place_id": "abc"},
        normalized_url="https://places.googleapis.com/v1/places/abc",
    )
    assert a == b
    assert len(a) == 64


def test_get_google_places_ttl_policy_for_success_uses_defaults():
    assert get_google_places_ttl_policy(status_code=200) == {
        "ttl_seconds": 21600,
        "refresh_after_seconds": 10800,
    }


def test_get_google_places_ttl_policy_allows_crawl_policy_override_on_success():
    assert get_google_places_ttl_policy(
        status_code=200,
        crawl_policy={"ttl_seconds": 7200, "refresh_after_seconds": 1800},
    ) == {"ttl_seconds": 7200, "refresh_after_seconds": 1800}


def test_get_google_places_ttl_policy_uses_short_ttl_for_429():
    assert get_google_places_ttl_policy(status_code=429) == {
        "ttl_seconds": 600,
        "refresh_after_seconds": None,
    }


def test_get_google_places_ttl_policy_uses_short_ttl_for_403():
    assert get_google_places_ttl_policy(status_code=403) == {
        "ttl_seconds": 7200,
        "refresh_after_seconds": None,
    }


def test_get_google_places_ttl_policy_uses_short_ttl_for_500_and_timeout():
    assert get_google_places_ttl_policy(status_code=500) == {
        "ttl_seconds": 60,
        "refresh_after_seconds": None,
    }
    assert get_google_places_ttl_policy(status_code=None, error_kind="timeout") == {
        "ttl_seconds": 60,
        "refresh_after_seconds": None,
    }


def test_fetch_place_detail_returns_cache_hit_without_calling_client():
    now = datetime(2026, 4, 22, 10, 0, tzinfo=timezone.utc)
    from food_data_ingestion.storage.cache_repository import build_cache_key
    cache_key = build_cache_key(
        "google_places",
        "place_detail",
        f"abc|{','.join(ESSENTIALS_DETAIL_FIELDS)}|zh-TW",
    )
    entry = ApiRequestCacheEntry(
        cache_key=cache_key,
        provider="google_places",
        resource_type="place_detail",
        request_fingerprint="f" * 64,
        request_params={"place_id": "abc"},
        normalized_url="https://places.googleapis.com/v1/places/abc",
        status_code=200,
        response_headers={"etag": "1"},
        response_body={"id": "abc", "displayName": {"text": "店家"}},
        fetched_at=now - timedelta(minutes=5),
        refresh_after=now + timedelta(hours=1),
        expires_at=now + timedelta(hours=2),
        source_meta={"cache_hit": True},
    )
    cache = FakeCacheRepository(entry)
    client = FakeGooglePlacesClient()
    connector = _connector(cache=cache, client=client, now=now)

    result = connector.fetch_place_detail("abc")

    assert result["cache_key"] == cache_key
    assert result["response_body"] == {"id": "abc", "displayName": {"text": "店家"}}
    assert result["source_meta"]["cache_hit"] is True
    assert client.detail_calls == []
    assert cache.mark_hits == [(cache_key, now)]


def test_fetch_place_detail_calls_client_on_cache_miss_and_returns_fetch_result():
    now = datetime(2026, 4, 22, 10, 0, tzinfo=timezone.utc)
    cache = FakeCacheRepository(None)
    client = FakeGooglePlacesClient(
        detail_response={
            "status_code": 200,
            "headers": {"etag": "v1"},
            "json_body": {"id": "abc", "displayName": {"text": "店家"}},
            "text_body": '{"id":"abc"}',
        }
    )
    connector = _connector(cache=cache, client=client, now=now)

    result = connector.fetch_place_detail("abc")

    assert client.detail_calls == [("abc", ESSENTIALS_DETAIL_FIELDS, "zh-TW")]
    assert result["status_code"] == 200
    assert result["resource_type"] == "place_detail"
    assert result["response_body"] == {"id": "abc", "displayName": {"text": "店家"}}
    assert result["is_error"] is False
    assert result["refresh_after"] == now + timedelta(hours=3)
    assert result["expires_at"] == now + timedelta(hours=6)
    assert result["source_meta"]["cache_hit"] is False
    assert len(cache.upserts) == 1


def test_fetch_place_detail_returns_error_fetch_result_with_short_ttl():
    now = datetime(2026, 4, 22, 10, 0, tzinfo=timezone.utc)
    cache = FakeCacheRepository(None)
    client = FakeGooglePlacesClient(
        detail_response={
            "status_code": 429,
            "headers": {"retry-after": "120"},
            "json_body": {"error": {"code": 429, "message": "quota exceeded"}},
            "text_body": '{"error":{"message":"quota exceeded"}}',
        }
    )
    connector = _connector(cache=cache, client=client, now=now)

    result = connector.fetch_place_detail("abc")

    assert result["is_error"] is True
    assert result["status_code"] == 429
    assert result["error_message"] == "quota exceeded"
    assert result["expires_at"] == now + timedelta(minutes=10)
    assert cache.upserts[0].is_error is True


def test_search_text_calls_client_with_essentials_fields_and_caches():
    now = datetime(2026, 4, 22, 10, 0, tzinfo=timezone.utc)
    cache = FakeCacheRepository(None)
    client = FakeGooglePlacesClient(
        search_response={
            "status_code": 200,
            "headers": {},
            "json_body": {
                "places": [
                    {
                        "id": "ChIJ1",
                        "displayName": {"text": "鼎泰豐 信義店"},
                        "formattedAddress": "台北市信義區松高路19號",
                        "location": {"latitude": 25.04, "longitude": 121.56},
                    }
                ]
            },
            "text_body": "{}",
        }
    )
    connector = _connector(cache=cache, client=client, now=now)

    result = connector.search_text("鼎泰豐 信義店")

    assert client.search_calls == [("鼎泰豐 信義店", ESSENTIALS_SEARCH_FIELDS, "zh-TW", "tw")]
    assert result["resource_type"] == "text_search"
    assert result["status_code"] == 200
    assert result["response_body"]["places"][0]["id"] == "ChIJ1"
    assert "text_search" in result["cache_key"]
    assert len(cache.upserts) == 1


def test_search_text_returns_cache_hit_without_calling_client():
    now = datetime(2026, 4, 22, 10, 0, tzinfo=timezone.utc)
    from food_data_ingestion.storage.cache_repository import build_cache_key
    cache_key = build_cache_key(
        "google_places",
        "text_search",
        f"鼎泰豐|{','.join(ESSENTIALS_SEARCH_FIELDS)}|zh-TW|tw",
    )
    entry = ApiRequestCacheEntry(
        cache_key=cache_key,
        provider="google_places",
        resource_type="text_search",
        request_fingerprint="f" * 64,
        request_params={"text_query": "鼎泰豐"},
        normalized_url="https://places.googleapis.com/v1/places:searchText",
        status_code=200,
        response_headers={},
        response_body={"places": [{"id": "ChIJ1"}]},
        fetched_at=now - timedelta(minutes=5),
        refresh_after=now + timedelta(hours=1),
        expires_at=now + timedelta(hours=2),
        source_meta={"cache_hit": True},
    )
    cache = FakeCacheRepository(entry)
    client = FakeGooglePlacesClient()
    connector = _connector(cache=cache, client=client, now=now)

    result = connector.search_text("鼎泰豐")

    assert result["source_meta"]["cache_hit"] is True
    assert client.search_calls == []
    assert result["response_body"] == {"places": [{"id": "ChIJ1"}]}
