from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from food_data_ingestion.connectors.supertaste import (
    SupertasteConnector,
    SupertasteLiveFetcher,
    DEFAULT_SITEMAP_INDEX_URL,
)
from food_data_ingestion.models.cache import ApiRequestCacheEntry


class FakeCacheRepository:
    def __init__(self, entry: ApiRequestCacheEntry | None = None):
        self.entry = entry
        self.get_calls = []
        self.upserts = []
        self.mark_hits = []

    def get_valid(self, cache_key, *, as_of):
        self.get_calls.append((cache_key, as_of))
        return self.entry

    def upsert(self, entry):
        self.upserts.append(entry)

    def mark_hit(self, cache_key, *, accessed_at):
        self.mark_hits.append((cache_key, accessed_at))


class FakeHTTPClient:
    def __init__(self, *, body: str = ""):
        self.body = body
        self.calls = []

    def fetch_text(self, url, *, headers, timeout):
        self.calls.append((url, dict(headers), timeout))
        return self.body


NOW = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)


def _make_connector(*, cache_repository, body=""):
    fetcher = SupertasteLiveFetcher(http_client=FakeHTTPClient(body=body))
    return (
        SupertasteConnector(
            cache_repository=cache_repository,
            fetcher=fetcher,
            now_provider=lambda: NOW,
        ),
        fetcher.http_client,
    )


def test_fetch_sitemap_index_cache_miss_persists_text_and_uses_default_url():
    cache = FakeCacheRepository(None)
    connector, http = _make_connector(cache_repository=cache, body="<sitemapindex/>")

    result = connector.fetch_sitemap_index()

    assert result["resource_type"] == "sitemap_index"
    assert result["normalized_url"] == DEFAULT_SITEMAP_INDEX_URL
    assert result["response_text"] == "<sitemapindex/>"
    assert result["source_meta"]["cache_hit"] is False
    assert result["expires_at"] == NOW + timedelta(seconds=3600)
    assert http.calls and http.calls[0][0] == DEFAULT_SITEMAP_INDEX_URL
    assert len(cache.upserts) == 1


def test_fetch_sitemap_index_cache_hit_skips_http_and_marks_hit():
    cached = ApiRequestCacheEntry(
        cache_key="supertaste:v1:sitemap_index:https---supertaste-tvbs-com-tw-supertaste_sitemap-sitemap-xml",
        provider="supertaste",
        resource_type="sitemap_index",
        request_fingerprint="fp",
        request_params={"sitemap_index_url": DEFAULT_SITEMAP_INDEX_URL},
        normalized_url=DEFAULT_SITEMAP_INDEX_URL,
        status_code=200,
        response_headers=None,
        response_body=None,
        response_text="<sitemapindex>cached</sitemapindex>",
        fetched_at=NOW - timedelta(minutes=10),
        refresh_after=None,
        expires_at=NOW + timedelta(hours=1),
        source_meta={},
    )
    cache = FakeCacheRepository(cached)
    connector, http = _make_connector(cache_repository=cache, body="should-not-fetch")

    result = connector.fetch_sitemap_index()

    assert result["response_text"] == "<sitemapindex>cached</sitemapindex>"
    assert result["source_meta"]["cache_hit"] is True
    assert http.calls == []
    assert cache.upserts == []
    assert len(cache.mark_hits) == 1


def test_fetch_sitemap_uses_url_as_identifier():
    cache = FakeCacheRepository(None)
    connector, http = _make_connector(cache_repository=cache, body="<urlset/>")

    url = "https://supertaste.tvbs.com.tw/supertaste_sitemap/article_sitemap_12.xml"
    result = connector.fetch_sitemap(url)

    assert result["resource_type"] == "sitemap"
    assert result["normalized_url"] == url
    assert http.calls[0][0] == url


def test_fetch_article_decodes_json_into_response_body_and_uses_article_ttl():
    cache = FakeCacheRepository(None)
    body = json.dumps({"data": {"articles_id": "348872", "title": "x"}})
    connector, http = _make_connector(cache_repository=cache, body=body)

    result = connector.fetch_article("pack", "348872", crawl_policy={"ttl_seconds": 60})

    assert result["resource_type"] == "article"
    assert result["normalized_url"].endswith("/api/article/pack/348872")
    assert isinstance(result["response_body"], dict)
    assert result["response_body"]["data"]["articles_id"] == "348872"
    assert result["response_text"] is None
    assert result["expires_at"] == NOW + timedelta(seconds=60)
    # Cache row should hold the decoded JSON, not the raw text
    assert cache.upserts[0].response_body == {"data": {"articles_id": "348872", "title": "x"}}
    assert cache.upserts[0].response_text is None
