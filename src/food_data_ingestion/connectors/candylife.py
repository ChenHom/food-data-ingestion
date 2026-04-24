from __future__ import annotations

import urllib.request
from dataclasses import dataclass
from typing import Protocol


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
        feed_url = url or 'https://candylife.tw/feed/'
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
