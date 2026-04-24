from __future__ import annotations

from food_data_ingestion.parsers.candylife_feed import (
    ArticleKind,
    filter_recent_entries,
    parse_candylife_feed,
)


SAMPLE_FEED = """
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:dc="http://purl.org/dc/elements/1.1/">
  <channel>
    <item>
      <title>阿發現炒｜這間比排隊名店還厲害？第二市場隱藏版美食，炒麵配當歸腦髓湯，內行人必點！</title>
      <link>https://candylife.tw/afafreshlyfried/</link>
      <pubDate>Thu, 23 Apr 2026 06:24:32 +0000</pubDate>
      <category>台中小吃</category>
      <category>台中美食</category>
    </item>
    <item>
      <title>台中乳酪蛋糕懶人包｜四間在地人激推最強乳酪蛋糕，乳酪控必收藏！</title>
      <link>https://candylife.tw/bakedcheesecakebag/</link>
      <pubDate>Mon, 20 Apr 2026 13:23:56 +0000</pubDate>
      <category>懶人包特輯</category>
      <category>台中美食</category>
    </item>
    <item>
      <title>2024 舊文章｜測試用</title>
      <link>https://candylife.tw/old-post/</link>
      <pubDate>Mon, 20 May 2024 13:23:56 +0000</pubDate>
      <category>台中美食</category>
    </item>
  </channel>
</rss>
"""


def test_parse_candylife_feed_parses_entries_and_classifies_article_kind():
    entries = parse_candylife_feed(SAMPLE_FEED)

    assert len(entries) == 3
    assert entries[0].title.startswith("阿發現炒")
    assert entries[0].article_kind is ArticleKind.SINGLE_STORE
    assert entries[1].article_kind is ArticleKind.ROUNDUP
    assert entries[2].article_kind is ArticleKind.SINGLE_STORE


def test_filter_recent_entries_keeps_only_2025_and_newer():
    entries = parse_candylife_feed(SAMPLE_FEED)

    filtered = filter_recent_entries(entries, min_year=2025)

    assert [entry.link for entry in filtered] == [
        "https://candylife.tw/afafreshlyfried/",
        "https://candylife.tw/bakedcheesecakebag/",
    ]
