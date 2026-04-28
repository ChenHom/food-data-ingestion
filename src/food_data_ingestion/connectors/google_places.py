"""Google Places API (New) connector — Essentials/Pro 雙階段、走 cache。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
import json
from typing import Any, Protocol
from urllib.request import Request, urlopen

from food_data_ingestion.config import Settings
from food_data_ingestion.connectors.base import CacheRepositoryProtocol, FetchResult
from food_data_ingestion.models.cache import ApiRequestCacheEntry
from food_data_ingestion.storage.cache_repository import build_cache_key

GOOGLE_PLACES_PROVIDER = "google_places"
RESOURCE_PLACE_DETAIL = "place_detail"
RESOURCE_TEXT_SEARCH = "text_search"

ESSENTIALS_DETAIL_FIELDS = (
    "id",
    "displayName",
    "formattedAddress",
    "location",
    "types",
)
ESSENTIALS_SEARCH_FIELDS = tuple(f"places.{f}" for f in ESSENTIALS_DETAIL_FIELDS)

PRO_DETAIL_FIELDS = (
    "rating",
    "userRatingCount",
    "regularOpeningHours",
    "websiteUri",
    "internationalPhoneNumber",
    "priceLevel",
    "businessStatus",
)


@dataclass(frozen=True)
class GooglePlacesHttpResponse:
    status_code: int
    headers: dict[str, Any] | None
    json_body: dict[str, Any] | list[Any] | None
    text_body: str | None


class GooglePlacesClientProtocol(Protocol):
    def fetch_place_detail(
        self, *, place_id: str, field_mask: tuple[str, ...], language_code: str
    ) -> GooglePlacesHttpResponse | dict[str, Any]: ...

    def search_text(
        self,
        *,
        text_query: str,
        field_mask: tuple[str, ...],
        language_code: str,
        region_code: str | None,
    ) -> GooglePlacesHttpResponse | dict[str, Any]: ...


class GooglePlacesApiClient:
    BASE_URL = "https://places.googleapis.com/v1"

    def __init__(self, api_key: str, *, timeout: int = 15) -> None:
        self.api_key = api_key
        self.timeout = timeout

    def _request(
        self,
        *,
        method: str,
        path: str,
        field_mask: tuple[str, ...],
        body: dict[str, Any] | None = None,
    ) -> GooglePlacesHttpResponse:
        url = f"{self.BASE_URL}{path}"
        headers = {
            "X-Goog-Api-Key": self.api_key,
            "X-Goog-FieldMask": ",".join(field_mask),
            "Accept": "application/json",
        }
        data: bytes | None = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(body).encode("utf-8")
        request = Request(url, data=data, headers=headers, method=method)
        with urlopen(request, timeout=self.timeout) as response:
            text_body = response.read().decode("utf-8")
            json_body = json.loads(text_body) if text_body else None
            return GooglePlacesHttpResponse(
                status_code=response.status,
                headers=dict(response.headers.items()),
                json_body=json_body,
                text_body=text_body,
            )

    def fetch_place_detail(
        self, *, place_id: str, field_mask: tuple[str, ...], language_code: str
    ) -> GooglePlacesHttpResponse:
        path = f"/places/{place_id}?languageCode={language_code}"
        return self._request(method="GET", path=path, field_mask=field_mask)

    def search_text(
        self,
        *,
        text_query: str,
        field_mask: tuple[str, ...],
        language_code: str,
        region_code: str | None,
    ) -> GooglePlacesHttpResponse:
        body: dict[str, Any] = {"textQuery": text_query, "languageCode": language_code}
        if region_code:
            body["regionCode"] = region_code
        return self._request(
            method="POST", path="/places:searchText", field_mask=field_mask, body=body
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
        now_provider: Any = None,
    ) -> None:
        self.settings = settings
        self.cache_repository = cache_repository
        self.client = client or GooglePlacesApiClient(settings.google_places_api_key)
        self.now_provider = now_provider or (lambda: datetime.now(UTC))

    def fetch_place_detail(
        self,
        place_id: str,
        *,
        field_mask: tuple[str, ...] = ESSENTIALS_DETAIL_FIELDS,
        language_code: str = "zh-TW",
        crawl_policy: dict[str, Any] | None = None,
    ) -> FetchResult:
        request_params: dict[str, Any] = {
            "place_id": place_id,
            "field_mask": list(field_mask),
            "language_code": language_code,
        }
        normalized_url = f"https://places.googleapis.com/v1/places/{place_id}"
        cache_key = build_cache_key(
            GOOGLE_PLACES_PROVIDER,
            RESOURCE_PLACE_DETAIL,
            f"{place_id}|{','.join(field_mask)}|{language_code}",
        )
        return self._fetch_with_cache(
            resource_type=RESOURCE_PLACE_DETAIL,
            cache_key=cache_key,
            normalized_url=normalized_url,
            request_params=request_params,
            crawl_policy=crawl_policy,
            do_request=lambda: self.client.fetch_place_detail(
                place_id=place_id, field_mask=field_mask, language_code=language_code
            ),
        )

    def search_text(
        self,
        text_query: str,
        *,
        field_mask: tuple[str, ...] = ESSENTIALS_SEARCH_FIELDS,
        language_code: str = "zh-TW",
        region_code: str | None = "tw",
        crawl_policy: dict[str, Any] | None = None,
    ) -> FetchResult:
        request_params: dict[str, Any] = {
            "text_query": text_query,
            "field_mask": list(field_mask),
            "language_code": language_code,
            "region_code": region_code,
        }
        normalized_url = "https://places.googleapis.com/v1/places:searchText"
        cache_key = build_cache_key(
            GOOGLE_PLACES_PROVIDER,
            RESOURCE_TEXT_SEARCH,
            f"{text_query}|{','.join(field_mask)}|{language_code}|{region_code or ''}",
        )
        return self._fetch_with_cache(
            resource_type=RESOURCE_TEXT_SEARCH,
            cache_key=cache_key,
            normalized_url=normalized_url,
            request_params=request_params,
            crawl_policy=crawl_policy,
            do_request=lambda: self.client.search_text(
                text_query=text_query,
                field_mask=field_mask,
                language_code=language_code,
                region_code=region_code,
            ),
        )

    def _fetch_with_cache(
        self,
        *,
        resource_type: str,
        cache_key: str,
        normalized_url: str,
        request_params: dict[str, Any],
        crawl_policy: dict[str, Any] | None,
        do_request: Any,
    ) -> FetchResult:
        request_fingerprint = build_request_fingerprint(
            provider=GOOGLE_PLACES_PROVIDER,
            resource_type=resource_type,
            request_params=request_params,
            normalized_url=normalized_url,
        )
        now = self.now_provider()

        cache_entry = self.cache_repository.get_valid(cache_key, as_of=now)
        if cache_entry is not None:
            self.cache_repository.mark_hit(cache_key, accessed_at=now)
            return {
                "provider": GOOGLE_PLACES_PROVIDER,
                "resource_type": resource_type,
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

        response = do_request()
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

        error_kind = "timeout" if response_body is None and response_text is None else None
        ttl_policy = get_google_places_ttl_policy(
            status_code=status_code,
            crawl_policy=crawl_policy,
            error_kind=error_kind,
        )
        expires_at = now + timedelta(seconds=int(ttl_policy["ttl_seconds"] or 0))
        refresh_after_seconds = ttl_policy["refresh_after_seconds"]
        refresh_after = (
            now + timedelta(seconds=refresh_after_seconds) if refresh_after_seconds is not None else None
        )

        error_message = None
        is_error = status_code is None or status_code >= 400
        if isinstance(response_body, dict):
            err = response_body.get("error")
            if isinstance(err, dict):
                error_message = err.get("message")
        if is_error and error_message is None:
            error_message = f"google places request failed: status={status_code}"

        source_meta = {"cache_hit": False, "request_fingerprint": request_fingerprint}
        self.cache_repository.upsert(
            ApiRequestCacheEntry(
                cache_key=cache_key,
                provider=GOOGLE_PLACES_PROVIDER,
                resource_type=resource_type,
                request_fingerprint=request_fingerprint,
                request_params=request_params,
                normalized_url=normalized_url,
                status_code=status_code,
                response_headers=response_headers,
                response_body=response_body,
                response_text=response_text,
                fetched_at=now,
                refresh_after=refresh_after,
                expires_at=expires_at,
                last_accessed_at=now,
                is_error=is_error,
                error_message=error_message,
                source_meta=source_meta,
            )
        )

        return {
            "provider": GOOGLE_PLACES_PROVIDER,
            "resource_type": resource_type,
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
            "source_meta": source_meta,
        }
