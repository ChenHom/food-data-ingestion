"""Supertaste TVBS connector。

提供三個走 cache 的 fetch 方法，回傳 `FetchResult`：
  - fetch_sitemap_index() — 根層 sitemap index XML
  - fetch_sitemap(url)     — 子層 article sitemap XML
  - fetch_article(category, article_id) — 透過 /api/article/{cat}/{id} 取得 JSON

與 `connectors/candylife.py` 對應：connector 自行負責 cache 的讀寫，TTL
可以透過 `crawl_policy={'ttl_seconds': N}` 覆寫。
"""

from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Protocol

from food_data_ingestion.connectors.base import CacheRepositoryProtocol, FetchResult
from food_data_ingestion.models.cache import ApiRequestCacheEntry
from food_data_ingestion.storage.cache_repository import build_cache_key


SUPERTASTE_PROVIDER = "supertaste"
SITEMAP_INDEX_RESOURCE = "sitemap_index"
SITEMAP_RESOURCE = "sitemap"
ARTICLE_RESOURCE = "article"

DEFAULT_BASE_URL = "https://supertaste.tvbs.com.tw"
DEFAULT_SITEMAP_INDEX_URL = f"{DEFAULT_BASE_URL}/supertaste_sitemap/sitemap.xml"

DEFAULT_SITEMAP_INDEX_TTL_SECONDS = 3600
DEFAULT_SITEMAP_TTL_SECONDS = 3600
DEFAULT_ARTICLE_TTL_SECONDS = 7 * 86400


DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": "https://supertaste.tvbs.com.tw/",
}


class HTTPClientProtocol(Protocol):
    def fetch_text(self, url: str, *, headers: dict[str, str], timeout: int) -> str: ...


class SupertasteFetcherProtocol(Protocol):
    """任何能依此契約抓 sitemap/article 的物件都可注入 SupertasteConnector
    （含正式 SupertasteLiveFetcher 與測試用 stub fetcher）。"""

    base_url: str

    def fetch_sitemap_index(self, url: str | None = None) -> str: ...

    def fetch_sitemap(self, url: str) -> str: ...

    def fetch_article(self, category: str, article_id: str) -> str: ...


@dataclass
class UrllibHTTPClient:
    def fetch_text(self, url: str, *, headers: dict[str, str], timeout: int) -> str:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.read().decode("utf-8", "ignore")


@dataclass
class SupertasteLiveFetcher:
    http_client: HTTPClientProtocol | None = None
    timeout: int = 20
    base_url: str = DEFAULT_BASE_URL

    def __post_init__(self) -> None:
        if self.http_client is None:
            self.http_client = UrllibHTTPClient()

    def fetch_sitemap_index(self, url: str | None = None) -> str:
        return self.http_client.fetch_text(
            url or DEFAULT_SITEMAP_INDEX_URL,
            headers=DEFAULT_HEADERS,
            timeout=self.timeout,
        )

    def fetch_sitemap(self, url: str) -> str:
        return self.http_client.fetch_text(url, headers=DEFAULT_HEADERS, timeout=self.timeout)

    def fetch_article(self, category: str, article_id: str) -> str:
        url = f"{self.base_url}/api/article/{category}/{article_id}"
        return self.http_client.fetch_text(url, headers=DEFAULT_HEADERS, timeout=self.timeout)


def _resolve_ttl(default_seconds: int, *, crawl_policy: dict[str, Any] | None) -> int:
    if crawl_policy and "ttl_seconds" in crawl_policy:
        return int(crawl_policy["ttl_seconds"])
    return default_seconds


class SupertasteConnector:
    """圍繞 SupertasteLiveFetcher 的 cache 與 FetchResult 包裝層。"""

    def __init__(
        self,
        *,
        cache_repository: CacheRepositoryProtocol,
        fetcher: SupertasteFetcherProtocol | None = None,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self.cache_repository = cache_repository
        self.fetcher = fetcher or SupertasteLiveFetcher()
        self.now_provider = now_provider or (lambda: datetime.now(UTC))

    def fetch_sitemap_index(
        self,
        url: str | None = None,
        *,
        crawl_policy: dict[str, Any] | None = None,
    ) -> FetchResult:
        target = url or DEFAULT_SITEMAP_INDEX_URL
        return self._fetch(
            resource_type=SITEMAP_INDEX_RESOURCE,
            identifier=target,
            normalized_url=target,
            request_params={"sitemap_index_url": target},
            ttl_seconds=_resolve_ttl(DEFAULT_SITEMAP_INDEX_TTL_SECONDS, crawl_policy=crawl_policy),
            fetch_callable=lambda: self.fetcher.fetch_sitemap_index(target),
            body_kind="text",
        )

    def fetch_sitemap(
        self,
        url: str,
        *,
        crawl_policy: dict[str, Any] | None = None,
    ) -> FetchResult:
        return self._fetch(
            resource_type=SITEMAP_RESOURCE,
            identifier=url,
            normalized_url=url,
            request_params={"sitemap_url": url},
            ttl_seconds=_resolve_ttl(DEFAULT_SITEMAP_TTL_SECONDS, crawl_policy=crawl_policy),
            fetch_callable=lambda: self.fetcher.fetch_sitemap(url),
            body_kind="text",
        )

    def fetch_article(
        self,
        category: str,
        article_id: str,
        *,
        crawl_policy: dict[str, Any] | None = None,
    ) -> FetchResult:
        identifier = f"{category}/{article_id}"
        api_url = f"{self.fetcher.base_url}/api/article/{identifier}"
        return self._fetch(
            resource_type=ARTICLE_RESOURCE,
            identifier=identifier,
            normalized_url=api_url,
            request_params={"category": category, "article_id": article_id},
            ttl_seconds=_resolve_ttl(DEFAULT_ARTICLE_TTL_SECONDS, crawl_policy=crawl_policy),
            fetch_callable=lambda: self.fetcher.fetch_article(category, article_id),
            body_kind="json",
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
        cache_key = build_cache_key(SUPERTASTE_PROVIDER, resource_type, identifier)
        now = self.now_provider()

        cache_entry = self.cache_repository.get_valid(cache_key, as_of=now)
        if cache_entry is not None:
            self.cache_repository.mark_hit(cache_key, accessed_at=now)
            return {
                "provider": SUPERTASTE_PROVIDER,
                "resource_type": resource_type,
                "cache_key": cache_key,
                "normalized_url": cache_entry.normalized_url,
                "request_params": cache_entry.request_params,
                "status_code": cache_entry.status_code,
                "response_headers": cache_entry.response_headers,
                "response_body": cache_entry.response_body,
                "response_text": cache_entry.response_text if body_kind == "text" else None,
                "response_html": None,
                "fetched_at": cache_entry.fetched_at,
                "expires_at": cache_entry.expires_at,
                "refresh_after": cache_entry.refresh_after,
                "is_error": cache_entry.is_error,
                "error_message": cache_entry.error_message,
                "source_meta": {**(cache_entry.source_meta or {}), "cache_hit": True},
            }

        text: str | None
        is_error = False
        error_message: str | None = None
        status_code: int | None = 200
        try:
            text = fetch_callable()
        except Exception as exc:  # pragma: no cover - thin error path
            text = None
            is_error = True
            error_message = str(exc)
            status_code = None

        response_body: dict[str, Any] | list[Any] | None = None
        response_text: str | None = None
        if body_kind == "json" and text is not None:
            try:
                parsed = json.loads(text)
                if isinstance(parsed, (dict, list)):
                    response_body = parsed
                else:
                    response_text = text
            except json.JSONDecodeError as exc:
                is_error = True
                error_message = f"json 解析失敗：{exc}"
                response_text = text
        else:
            response_text = text

        expires_at = now + timedelta(seconds=ttl_seconds)
        source_meta: dict[str, Any] = {"cache_hit": False}

        self.cache_repository.upsert(
            ApiRequestCacheEntry(
                cache_key=cache_key,
                provider=SUPERTASTE_PROVIDER,
                resource_type=resource_type,
                request_fingerprint=cache_key,
                request_params=request_params,
                normalized_url=normalized_url,
                status_code=status_code,
                response_headers=None,
                response_body=response_body,
                response_text=response_text,
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
            "provider": SUPERTASTE_PROVIDER,
            "resource_type": resource_type,
            "cache_key": cache_key,
            "normalized_url": normalized_url,
            "request_params": request_params,
            "status_code": status_code,
            "response_headers": None,
            "response_body": response_body,
            "response_text": response_text,
            "response_html": None,
            "fetched_at": now,
            "expires_at": expires_at,
            "refresh_after": None,
            "is_error": is_error,
            "error_message": error_message,
            "source_meta": source_meta,
        }
