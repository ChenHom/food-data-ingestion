from __future__ import annotations

import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Protocol

from food_data_ingestion.connectors.base import CacheRepositoryProtocol, FetchResult
from food_data_ingestion.models.cache import ApiRequestCacheEntry
from food_data_ingestion.storage.cache_repository import build_cache_key


CANDYLIFE_PROVIDER = "candylife"
CANDYLIFE_FEED_RESOURCE = "feed"
CANDYLIFE_ARTICLE_RESOURCE = "article"
DEFAULT_FEED_URL = "https://candylife.tw/feed/"

# TTL 預設值：feed 每小時重新整理一次；article 快取一週。
DEFAULT_FEED_TTL_SECONDS = 3600
DEFAULT_ARTICLE_TTL_SECONDS = 7 * 86400


DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
}


class HTTPClientProtocol(Protocol):
    def fetch_text(self, url: str, *, headers: dict[str, str], timeout: int) -> str: ...


@dataclass
class UrllibHTTPClient:
    def fetch_text(self, url: str, *, headers: dict[str, str], timeout: int) -> str:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.read().decode('utf-8', 'ignore')


@dataclass
class CandylifeLiveFetcher:
    http_client: HTTPClientProtocol | None = None
    timeout: int = 20

    def __post_init__(self) -> None:
        if self.http_client is None:
            self.http_client = UrllibHTTPClient()

    def fetch_feed(self, url: str | None = None) -> str:
        feed_url = url or DEFAULT_FEED_URL
        return self.http_client.fetch_text(
            feed_url,
            headers={**DEFAULT_HEADERS, 'Referer': 'https://candylife.tw/'},
            timeout=self.timeout,
        )

    def fetch_html(self, url: str) -> str:
        return self.http_client.fetch_text(
            url,
            headers={**DEFAULT_HEADERS, 'Referer': 'https://candylife.tw/'},
            timeout=self.timeout,
        )


def _resolve_ttl(default_seconds: int, *, crawl_policy: dict[str, Any] | None) -> int:
    if crawl_policy and "ttl_seconds" in crawl_policy:
        return int(crawl_policy["ttl_seconds"])
    return default_seconds


class CandylifeConnector:
    """高階 connector：在 CandylifeLiveFetcher 之上加入 cache 與 FetchResult 契約。"""

    def __init__(
        self,
        *,
        cache_repository: CacheRepositoryProtocol,
        fetcher: CandylifeLiveFetcher | None = None,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self.cache_repository = cache_repository
        self.fetcher = fetcher or CandylifeLiveFetcher()
        self.now_provider = now_provider or (lambda: datetime.now(UTC))

    def fetch_feed(
        self,
        url: str | None = None,
        *,
        crawl_policy: dict[str, Any] | None = None,
    ) -> FetchResult:
        feed_url = url or DEFAULT_FEED_URL
        return self._fetch(
            resource_type=CANDYLIFE_FEED_RESOURCE,
            identifier=feed_url,
            normalized_url=feed_url,
            request_params={"feed_url": feed_url},
            ttl_seconds=_resolve_ttl(DEFAULT_FEED_TTL_SECONDS, crawl_policy=crawl_policy),
            fetch_callable=lambda: self.fetcher.fetch_feed(feed_url),
            body_kind="text",
        )

    def fetch_article(
        self,
        url: str,
        *,
        crawl_policy: dict[str, Any] | None = None,
    ) -> FetchResult:
        return self._fetch(
            resource_type=CANDYLIFE_ARTICLE_RESOURCE,
            identifier=url,
            normalized_url=url,
            request_params={"article_url": url},
            ttl_seconds=_resolve_ttl(DEFAULT_ARTICLE_TTL_SECONDS, crawl_policy=crawl_policy),
            fetch_callable=lambda: self.fetcher.fetch_html(url),
            body_kind="html",
        )

    def _fetch(
        self,
        *,
        resource_type: str,
        identifier: str,
        normalized_url: str,
        request_params: dict[str, Any],
        ttl_seconds: int,
        fetch_callable: Callable[[], str],
        body_kind: str,
    ) -> FetchResult:
        cache_key = build_cache_key(CANDYLIFE_PROVIDER, resource_type, identifier)
        now = self.now_provider()

        cache_entry = self.cache_repository.get_valid(cache_key, as_of=now)
        if cache_entry is not None:
            self.cache_repository.mark_hit(cache_key, accessed_at=now)
            cached_text = cache_entry.response_text
            return {
                "provider": CANDYLIFE_PROVIDER,
                "resource_type": resource_type,
                "cache_key": cache_key,
                "normalized_url": cache_entry.normalized_url,
                "request_params": cache_entry.request_params,
                "status_code": cache_entry.status_code,
                "response_headers": cache_entry.response_headers,
                "response_body": cache_entry.response_body,
                "response_text": cached_text if body_kind == "text" else None,
                "response_html": cached_text if body_kind == "html" else None,
                "fetched_at": cache_entry.fetched_at,
                "expires_at": cache_entry.expires_at,
                "refresh_after": cache_entry.refresh_after,
                "is_error": cache_entry.is_error,
                "error_message": cache_entry.error_message,
                "source_meta": {**(cache_entry.source_meta or {}), "cache_hit": True},
            }

        try:
            text = fetch_callable()
            is_error = False
            error_message = None
            status_code = 200
        except Exception as exc:  # pragma: no cover - thin error path
            text = None
            is_error = True
            error_message = str(exc)
            status_code = None

        expires_at = now + timedelta(seconds=ttl_seconds)
        source_meta: dict[str, Any] = {"cache_hit": False}

        self.cache_repository.upsert(
            ApiRequestCacheEntry(
                cache_key=cache_key,
                provider=CANDYLIFE_PROVIDER,
                resource_type=resource_type,
                request_fingerprint=cache_key,
                request_params=request_params,
                normalized_url=normalized_url,
                status_code=status_code,
                response_headers=None,
                response_body=None,
                response_text=text,
                fetched_at=now,
                refresh_after=None,
                expires_at=expires_at,
                last_accessed_at=now,
                is_error=is_error,
                error_message=error_message,
                source_meta=source_meta,
            )
        )

        return {
            "provider": CANDYLIFE_PROVIDER,
            "resource_type": resource_type,
            "cache_key": cache_key,
            "normalized_url": normalized_url,
            "request_params": request_params,
            "status_code": status_code,
            "response_headers": None,
            "response_body": None,
            "response_text": text if body_kind == "text" else None,
            "response_html": text if body_kind == "html" else None,
            "fetched_at": now,
            "expires_at": expires_at,
            "refresh_after": None,
            "is_error": is_error,
            "error_message": error_message,
            "source_meta": source_meta,
        }
