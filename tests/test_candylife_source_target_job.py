from __future__ import annotations

from food_data_ingestion.jobs.run_candylife_discovery import run_candylife_discovery


class FakeFetcher:
    def __init__(self):
        self.feed_calls = []

    def fetch_feed(self, url: str | None = None) -> str:
        self.feed_calls.append(url)
        return '''<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0"><channel>
          <item><title>阿發現炒｜單店</title><link>https://candylife.tw/a/</link><pubDate>Thu, 23 Apr 2026 06:24:32 +0000</pubDate><category>台中美食</category></item>
        </channel></rss>'''

    def fetch_html(self, url: str) -> str:
        return '<html><head><title>阿發現炒｜單店 - 糖糖\'s 享食生活</title></head><body><article><h1>阿發現炒｜單店</h1><p>《店家資訊》</p><p>店家：阿發現炒 電話：04-12345678 地址：台中市中區測試路1號 時間：10:00~18:00</p></article></body></html>'


def test_run_candylife_discovery_uses_source_target_feed_url_and_policy_override():
    fetcher = FakeFetcher()

    result = run_candylife_discovery(
        fetcher=fetcher,
        min_year=2024,
        limit=20,
        source_target={
            'id': 42,
            'target_value': 'https://candylife.tw/custom-feed.xml',
            'crawl_policy': {'min_year': 2026, 'limit': 1},
        },
    )

    assert fetcher.feed_calls == ['https://candylife.tw/custom-feed.xml']
    assert result['min_year'] == 2026
    assert result['limit'] == 1
    assert result['source_target_id'] == 42
    assert result['processed_entry_count'] == 1
