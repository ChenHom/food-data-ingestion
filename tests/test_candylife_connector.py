from __future__ import annotations

from datetime import datetime, timedelta, timezone

from food_data_ingestion.connectors.candylife import (
    CandylifeConnector,
    CandylifeLiveFetcher,
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
    def __init__(self, *, body: str = "<html/>"):
        self.body = body
        self.calls = []

    def fetch_text(self, url, *, headers, timeout):
        self.calls.append((url, dict(headers), timeout))
        return self.body


NOW = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)


def _make_connector(*, cache_repository, body="<html>x</html>"):
    fetcher = CandylifeLiveFetcher(http_client=FakeHTTPClient(body=body))
    return CandylifeConnector(
        cache_repository=cache_repository,
        fetcher=fetcher,
        now_provider=lambda: NOW,
    ), fetcher.http_client


def test_fetch_feed_returns_cache_hit_without_calling_fetcher():
    entry = ApiRequestCacheEntry(
        cache_key="candylife:v1:feed:https---candylife-tw-feed-",
        provider="candylife",
        resource_type="feed",
        request_fingerprint="fp",
        request_params={"feed_url": "https://candylife.tw/feed/"},
        normalized_url="https://candylife.tw/feed/",
        status_code=200,
        response_headers=None,
        response_body=None,
        response_text="<rss>cached</rss>",
        fetched_at=NOW - timedelta(minutes=5),
        refresh_after=None,
        expires_at=NOW + timedelta(hours=1),
        source_meta={},
    )
    cache_repository = FakeCacheRepository(entry)
    connector, http_client = _make_connector(cache_repository=cache_repository)

    result = connector.fetch_feed()

    assert result["response_text"] == "<rss>cached</rss>"
    assert result["source_meta"]["cache_hit"] is True
    assert http_client.calls == []
    assert cache_repository.upserts == []
    assert len(cache_repository.mark_hits) == 1


def test_fetch_feed_calls_fetcher_on_miss_and_upserts_cache():
    cache_repository = FakeCacheRepository(None)
    connector, http_client = _make_connector(cache_repository=cache_repository, body="<rss>fresh</rss>")

    result = connector.fetch_feed()

    assert result["provider"] == "candylife"
    assert result["resource_type"] == "feed"
    assert result["response_text"] == "<rss>fresh</rss>"
    assert result["source_meta"]["cache_hit"] is False
    assert result["expires_at"] == NOW + timedelta(seconds=3600)
    assert len(http_client.calls) == 1
    assert len(cache_repository.upserts) == 1
    assert cache_repository.upserts[0].response_text == "<rss>fresh</rss>"


def test_fetch_article_uses_article_ttl_default():
    cache_repository = FakeCacheRepository(None)
    connector, http_client = _make_connector(cache_repository=cache_repository, body="<html>article</html>")

    result = connector.fetch_article("https://candylife.tw/foo/")

    assert result["resource_type"] == "article"
    assert result["normalized_url"] == "https://candylife.tw/foo/"
    assert result["expires_at"] == NOW + timedelta(seconds=7 * 86400)
    assert http_client.calls[0][0] == "https://candylife.tw/foo/"


def test_fetch_article_respects_crawl_policy_ttl_override():
    cache_repository = FakeCacheRepository(None)
    connector, _ = _make_connector(cache_repository=cache_repository)

    result = connector.fetch_article(
        "https://candylife.tw/foo/",
        crawl_policy={"ttl_seconds": 60},
    )

    assert result["expires_at"] == NOW + timedelta(seconds=60)
