from __future__ import annotations

import json

from food_data_ingestion.jobs.run_candylife_discovery import run_candylife_discovery


class FakeFetcher:
    def __init__(self, feed_xml: str, html_by_url: dict[str, str]):
        self.feed_xml = feed_xml
        self.html_by_url = html_by_url
        self.feed_calls = 0
        self.html_calls = []

    def fetch_feed(self) -> str:
        self.feed_calls += 1
        return self.feed_xml

    def fetch_html(self, url: str) -> str:
        self.html_calls.append(url)
        return self.html_by_url[url]


def test_run_candylife_discovery_only_processes_2025_plus_single_store_entries():
    feed_xml = '''<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0"><channel>
      <item><title>阿發現炒｜單店</title><link>https://candylife.tw/a/</link><pubDate>Thu, 23 Apr 2026 06:24:32 +0000</pubDate><category>台中美食</category></item>
      <item><title>台中乳酪蛋糕懶人包｜四間</title><link>https://candylife.tw/b/</link><pubDate>Mon, 20 Apr 2026 13:23:56 +0000</pubDate><category>懶人包特輯</category></item>
      <item><title>2024 舊文章</title><link>https://candylife.tw/c/</link><pubDate>Mon, 20 May 2024 13:23:56 +0000</pubDate><category>台中美食</category></item>
    </channel></rss>'''
    html_by_url = {
        'https://candylife.tw/a/': '<html><head><title>阿發現炒｜單店 - 糖糖\'s 享食生活</title></head><body><article><h1>阿發現炒｜單店</h1><p>《店家資訊》</p><p>店家：阿發現炒 電話：04-12345678 地址：台中市中區測試路1號 時間：10:00~18:00</p></article></body></html>',
        'https://candylife.tw/b/': '<html><head><title>台中乳酪蛋糕懶人包 - 糖糖\'s 享食生活</title></head><body><article><h1>台中乳酪蛋糕懶人包</h1></article></body></html>',
    }
    fetcher = FakeFetcher(feed_xml, html_by_url)

    result = run_candylife_discovery(fetcher=fetcher, min_year=2025, limit=10)

    assert result['feed_entry_count'] == 3
    assert result['eligible_entry_count'] == 2
    assert result['processed_entry_count'] == 2
    assert result['single_store_count'] == 1
    assert result['roundup_count'] == 1
    assert result['candidate_count'] == 1
    assert result['articles'][0]['candidate_count'] == 1
    assert result['articles'][1]['candidate_count'] == 0
