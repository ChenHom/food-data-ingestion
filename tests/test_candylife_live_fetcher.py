from __future__ import annotations

from food_data_ingestion.connectors.candylife import CandylifeLiveFetcher


class FakeHTTPClient:
    def __init__(self):
        self.calls = []

    def fetch_text(self, url: str, *, headers: dict[str, str], timeout: int) -> str:
        self.calls.append({"url": url, "headers": headers, "timeout": timeout})
        if url.endswith('/feed/'):
            return '<rss version="2.0"><channel></channel></rss>'
        return '<html><body>ok</body></html>'


def test_live_fetcher_uses_browser_like_headers_for_feed_and_article():
    client = FakeHTTPClient()
    fetcher = CandylifeLiveFetcher(http_client=client)

    feed_xml = fetcher.fetch_feed()
    article_html = fetcher.fetch_html('https://candylife.tw/255labcafe/')

    assert feed_xml.startswith('<rss')
    assert article_html.startswith('<html')
    assert client.calls[0]['url'] == 'https://candylife.tw/feed/'
    assert 'Mozilla/5.0' in client.calls[0]['headers']['User-Agent']
    assert client.calls[1]['url'] == 'https://candylife.tw/255labcafe/'
    assert client.calls[1]['headers']['Referer'] == 'https://candylife.tw/'
