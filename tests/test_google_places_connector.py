from __future__ import annotations

from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json

from food_data_ingestion.config import Settings
from food_data_ingestion.connectors.google_places import (
    GooglePlacesConnector,
    build_request_fingerprint,
    get_google_places_ttl_policy,
)
from food_data_ingestion.models.cache import ApiRequestCacheEntry


class FakeCacheRepository:
    def __init__(self, entry: ApiRequestCacheEntry | None = None):
        self.entry = entry
        self.calls = []

    def get_valid(self, cache_key: str, *, as_of: datetime):
        self.calls.append((cache_key, as_of))
        return self.entry


class FakeGooglePlacesClient:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error
        self.calls = []

    def fetch_place_detail(self, *, place_id: str, fields: list[str], language: str):
        self.calls.append((place_id, fields, language))
        if self.error is not None:
            raise self.error
        return self.response


def test_build_request_fingerprint_is_stable_for_equivalent_params():
    fingerprint_a = build_request_fingerprint(
        provider="google_places",
        resource_type="place_detail",
        request_params={"place_id": "abc", "fields": ["name", "rating"], "language": "zh-TW"},
        normalized_url="https://maps.googleapis.com/place/details/json?place_id=abc",
    )
    fingerprint_b = build_request_fingerprint(
        provider="google_places",
        resource_type="place_detail",
        request_params={"language": "zh-TW", "fields": ["name", "rating"], "place_id": "abc"},
        normalized_url="https://maps.googleapis.com/place/details/json?place_id=abc",
    )

    assert fingerprint_a == fingerprint_b
    assert len(fingerprint_a) == 64


def test_get_google_places_ttl_policy_for_success_uses_defaults():
    policy = get_google_places_ttl_policy(status_code=200)

    assert policy == {"ttl_seconds": 21600, "refresh_after_seconds": 10800}


def test_get_google_places_ttl_policy_allows_crawl_policy_override_on_success():
    policy = get_google_places_ttl_policy(
        status_code=200,
        crawl_policy={"ttl_seconds": 7200, "refresh_after_seconds": 1800, "cooldown_seconds": 600},
    )

    assert policy == {"ttl_seconds": 7200, "refresh_after_seconds": 1800}


def test_get_google_places_ttl_policy_uses_short_ttl_for_429():
    policy = get_google_places_ttl_policy(status_code=429)

    assert policy == {"ttl_seconds": 600, "refresh_after_seconds": None}


def test_get_google_places_ttl_policy_uses_short_ttl_for_403():
    policy = get_google_places_ttl_policy(status_code=403)

    assert policy == {"ttl_seconds": 7200, "refresh_after_seconds": None}


def test_get_google_places_ttl_policy_uses_short_ttl_for_500_and_timeout():
    assert get_google_places_ttl_policy(status_code=500) == {"ttl_seconds": 60, "refresh_after_seconds": None}
    assert get_google_places_ttl_policy(status_code=None, error_kind="timeout") == {"ttl_seconds": 60, "refresh_after_seconds": None}


def test_fetch_place_detail_returns_cache_hit_without_calling_client():
    now = datetime(2026, 4, 22, 10, 0, tzinfo=timezone.utc)
    entry = ApiRequestCacheEntry(
        cache_key="google_places:v1:place_detail:abc",
        provider="google_places",
        resource_type="place_detail",
        request_fingerprint="f" * 64,
        request_params={"place_id": "abc", "fields": ["name"], "language": "zh-TW"},
        normalized_url="https://maps.googleapis.com/place/details/json?place_id=abc",
        status_code=200,
        response_headers={"etag": "1"},
        response_body={"result": {"name": "店家"}},
        fetched_at=now - timedelta(minutes=5),
        refresh_after=now + timedelta(hours=1),
        expires_at=now + timedelta(hours=2),
        source_meta={"cache_hit": True},
    )
    cache_repository = FakeCacheRepository(entry)
    client = FakeGooglePlacesClient()
    connector = GooglePlacesConnector(
        settings=Settings(google_places_api_key="test-key"),
        cache_repository=cache_repository,
        client=client,
        now_provider=lambda: now,
    )

    result = connector.fetch_place_detail("abc", fields=["name"], language="zh-TW")

    assert result["cache_key"] == "google_places:v1:place_detail:abc"
    assert result["response_body"] == {"result": {"name": "店家"}}
    assert result["source_meta"]["cache_hit"] is True
    assert client.calls == []


def test_fetch_place_detail_calls_client_on_cache_miss_and_returns_fetch_result():
    now = datetime(2026, 4, 22, 10, 0, tzinfo=timezone.utc)
    cache_repository = FakeCacheRepository(None)
    client = FakeGooglePlacesClient(
        response={
            "status_code": 200,
            "headers": {"etag": "v1"},
            "json_body": {"result": {"place_id": "abc", "name": "店家"}},
            "text_body": '{"result": {"place_id": "abc", "name": "店家"}}',
        }
    )
    connector = GooglePlacesConnector(
        settings=Settings(google_places_api_key="test-key"),
        cache_repository=cache_repository,
        client=client,
        now_provider=lambda: now,
    )

    result = connector.fetch_place_detail("abc", fields=["name", "rating"], language="zh-TW")

    assert client.calls == [("abc", ["name", "rating"], "zh-TW")]
    assert result["provider"] == "google_places"
    assert result["resource_type"] == "place_detail"
    assert result["cache_key"] == "google_places:v1:place_detail:abc"
    assert result["request_params"] == {"place_id": "abc", "fields": ["name", "rating"], "language": "zh-TW"}
    assert result["status_code"] == 200
    assert result["response_body"] == {"result": {"place_id": "abc", "name": "店家"}}
    assert result["is_error"] is False
    assert result["error_message"] is None
    assert result["refresh_after"] == now + timedelta(hours=3)
    assert result["expires_at"] == now + timedelta(hours=6)
    expected_fingerprint = sha256(
        json.dumps(
            {
                "normalized_url": "https://maps.googleapis.com/place/details/json?place_id=abc",
                "provider": "google_places",
                "request_params": {"fields": ["name", "rating"], "language": "zh-TW", "place_id": "abc"},
                "resource_type": "place_detail",
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    assert result["source_meta"]["request_fingerprint"] == expected_fingerprint
    assert result["source_meta"]["cache_hit"] is False


def test_fetch_place_detail_returns_error_fetch_result_with_short_ttl():
    now = datetime(2026, 4, 22, 10, 0, tzinfo=timezone.utc)
    cache_repository = FakeCacheRepository(None)
    client = FakeGooglePlacesClient(
        response={
            "status_code": 429,
            "headers": {"retry-after": "120"},
            "json_body": {"error_message": "quota exceeded"},
            "text_body": '{"error_message":"quota exceeded"}',
        }
    )
    connector = GooglePlacesConnector(
        settings=Settings(google_places_api_key="test-key"),
        cache_repository=cache_repository,
        client=client,
        now_provider=lambda: now,
    )

    result = connector.fetch_place_detail("abc")

    assert result["is_error"] is True
    assert result["status_code"] == 429
    assert result["error_message"] == "quota exceeded"
    assert result["refresh_after"] is None
    assert result["expires_at"] == now + timedelta(minutes=10)
