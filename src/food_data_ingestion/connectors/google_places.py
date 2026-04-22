from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
import json
from typing import Any, Protocol
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from food_data_ingestion.config import Settings
from food_data_ingestion.connectors.base import CacheRepositoryProtocol, FetchResult
from food_data_ingestion.storage.cache_repository import build_cache_key

GOOGLE_PLACES_PROVIDER = "google_places"
GOOGLE_PLACES_RESOURCE_TYPE = "place_detail"
DEFAULT_FIELDS = [
    "place_id",
    "name",
    "formatted_address",
    "geometry",
    "rating",
    "user_ratings_total",
    "opening_hours",
    "website",
    "url",
]


@dataclass(frozen=True)
class GooglePlacesHttpResponse:
    status_code: int
    headers: dict[str, Any] | None
    json_body: dict[str, Any] | list[Any] | None
    text_body: str | None


class GooglePlacesClientProtocol(Protocol):
    def fetch_place_detail(self, *, place_id: str, fields: list[str], language: str) -> GooglePlacesHttpResponse | dict[str, Any]: ...


class GooglePlacesApiClient:
    def __init__(self, api_key: str, *, timeout: int = 15) -> None:
        self.api_key = api_key
        self.timeout = timeout

    def fetch_place_detail(self, *, place_id: str, fields: list[str], language: str) -> GooglePlacesHttpResponse:
        query = urlencode(
            {
                "place_id": place_id,
                "fields": ",".join(fields),
                "language": language,
                "key": self.api_key,
            }
        )
        url = f"https://maps.googleapis.com/maps/api/place/details/json?{query}"
        request = Request(url, headers={"Accept": "application/json"})
        with urlopen(request, timeout=self.timeout) as response:
            text_body = response.read().decode("utf-8")
            json_body = json.loads(text_body)
            return GooglePlacesHttpResponse(
                status_code=response.status,
                headers=dict(response.headers.items()),
                json_body=json_body,
                text_body=text_body,
            )


def build_request_fingerprint(
    *,
    provider: str,
    resource_type: str,
    request_params: dict[str, Any],
    normalized_url: str | None,
) -> str:
    canonical = json.dumps(
        {
            "provider": provider,
            "resource_type": resource_type,
            "request_params": request_params,
            "normalized_url": normalized_url,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return sha256(canonical.encode("utf-8")).hexdigest()


def get_google_places_ttl_policy(
    *,
    status_code: int | None,
    crawl_policy: dict[str, Any] | None = None,
    error_kind: str | None = None,
) -> dict[str, int | None]:
    crawl_policy = crawl_policy or {}

    if error_kind == "timeout" or status_code is None or status_code >= 500:
        return {"ttl_seconds": 60, "refresh_after_seconds": None}
    if status_code == 429:
        return {"ttl_seconds": 600, "refresh_after_seconds": None}
    if status_code == 403:
        return {"ttl_seconds": 7200, "refresh_after_seconds": None}
    if 400 <= status_code < 500:
        return {"ttl_seconds": 600, "refresh_after_seconds": None}

    ttl_seconds = int(crawl_policy.get("ttl_seconds", 21600))
    refresh_after_seconds = crawl_policy.get("refresh_after_seconds", 10800)
    if refresh_after_seconds is not None:
        refresh_after_seconds = int(refresh_after_seconds)
    return {"ttl_seconds": ttl_seconds, "refresh_after_seconds": refresh_after_seconds}


class GooglePlacesConnector:
    def __init__(
        self,
        *,
        settings: Settings,
        cache_repository: CacheRepositoryProtocol,
        client: GooglePlacesClientProtocol | None = None,
        now_provider: callable | None = None,
    ) -> None:
        self.settings = settings
        self.cache_repository = cache_repository
        self.client = client or GooglePlacesApiClient(settings.google_places_api_key)
        self.now_provider = now_provider or (lambda: datetime.now(UTC))

    def fetch_place_detail(
        self,
        place_id: str,
        *,
        fields: list[str] | None = None,
        language: str = "zh-TW",
        crawl_policy: dict[str, Any] | None = None,
    ) -> FetchResult:
        fields = fields or list(DEFAULT_FIELDS)
        request_params = {"place_id": place_id, "fields": fields, "language": language}
        cache_key = build_cache_key(GOOGLE_PLACES_PROVIDER, GOOGLE_PLACES_RESOURCE_TYPE, place_id)
        normalized_url = f"https://maps.googleapis.com/place/details/json?place_id={place_id}"
        request_fingerprint = build_request_fingerprint(
            provider=GOOGLE_PLACES_PROVIDER,
            resource_type=GOOGLE_PLACES_RESOURCE_TYPE,
            request_params=request_params,
            normalized_url=normalized_url,
        )
        now = self.now_provider()

        cache_entry = self.cache_repository.get_valid(cache_key, as_of=now)
        if cache_entry is not None:
            return {
                "provider": GOOGLE_PLACES_PROVIDER,
                "resource_type": GOOGLE_PLACES_RESOURCE_TYPE,
                "cache_key": cache_key,
                "normalized_url": cache_entry.normalized_url or normalized_url,
                "request_params": cache_entry.request_params or request_params,
                "status_code": cache_entry.status_code,
                "response_headers": cache_entry.response_headers,
                "response_body": cache_entry.response_body,
                "response_text": cache_entry.response_text,
                "fetched_at": cache_entry.fetched_at or now,
                "expires_at": cache_entry.expires_at or now,
                "refresh_after": cache_entry.refresh_after,
                "is_error": cache_entry.is_error,
                "error_message": cache_entry.error_message,
                "source_meta": {
                    **cache_entry.source_meta,
                    "cache_hit": True,
                    "request_fingerprint": cache_entry.request_fingerprint or request_fingerprint,
                },
            }

        response = self.client.fetch_place_detail(place_id=place_id, fields=fields, language=language)
        if isinstance(response, dict):
            status_code = response.get("status_code")
            response_headers = response.get("headers")
            response_body = response.get("json_body")
            response_text = response.get("text_body")
        else:
            status_code = response.status_code
            response_headers = response.headers
            response_body = response.json_body
            response_text = response.text_body

        error_kind = None
        if response_body is None and response_text is None:
            error_kind = "timeout"
        ttl_policy = get_google_places_ttl_policy(
            status_code=status_code,
            crawl_policy=crawl_policy,
            error_kind=error_kind,
        )
        expires_at = now + timedelta(seconds=int(ttl_policy["ttl_seconds"] or 0))
        refresh_after_seconds = ttl_policy["refresh_after_seconds"]
        refresh_after = now + timedelta(seconds=refresh_after_seconds) if refresh_after_seconds is not None else None

        error_message = None
        is_error = status_code is None or status_code >= 400
        if isinstance(response_body, dict):
            error_message = response_body.get("error_message")
        if is_error and error_message is None:
            error_message = f"google places request failed: status={status_code}"

        return {
            "provider": GOOGLE_PLACES_PROVIDER,
            "resource_type": GOOGLE_PLACES_RESOURCE_TYPE,
            "cache_key": cache_key,
            "normalized_url": normalized_url,
            "request_params": request_params,
            "status_code": status_code,
            "response_headers": response_headers,
            "response_body": response_body,
            "response_text": response_text,
            "fetched_at": now,
            "expires_at": expires_at,
            "refresh_after": refresh_after,
            "is_error": is_error,
            "error_message": error_message,
            "source_meta": {
                "cache_hit": False,
                "request_fingerprint": request_fingerprint,
            },
        }
