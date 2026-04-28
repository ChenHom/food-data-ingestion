from __future__ import annotations

from pathlib import Path

from food_data_ingestion.parsers.supertaste_sitemap import (
    parse_supertaste_sitemap,
    parse_supertaste_sitemap_index,
)


FIXTURES = Path(__file__).parent / "fixtures" / "supertaste"


def test_parse_sitemap_index_keeps_only_article_sitemaps_by_default():
    xml = (FIXTURES / "sitemap_index.xml").read_text(encoding="utf-8")
    urls = parse_supertaste_sitemap_index(xml)
    assert all("article_sitemap_" in u for u in urls)
    assert urls[0].endswith("article_sitemap_1.xml")
    assert len(urls) >= 40


def test_parse_sitemap_index_can_return_all_sitemap_kinds():
    xml = (FIXTURES / "sitemap_index.xml").read_text(encoding="utf-8")
    all_urls = parse_supertaste_sitemap_index(xml, only_article=False)
    article_urls = parse_supertaste_sitemap_index(xml)
    assert any("tag_sitemap_" in u for u in all_urls)
    assert any("store_sitemap_" in u for u in all_urls)
    assert len(all_urls) > len(article_urls)


def test_parse_article_sitemap_extracts_category_and_id():
    xml = (FIXTURES / "article_sitemap_sample.xml").read_text(encoding="utf-8")
    entries = parse_supertaste_sitemap(xml)
    assert len(entries) == 8
    first = entries[0]
    assert first.url == "https://supertaste.tvbs.com.tw/hot/322011"
    assert first.category == "hot"
    assert first.article_id == "322011"
    assert first.lastmod == "2020-04-16T11:24:46+08:00"
    # fixture 裡觀察到的分類
    cats = {e.category for e in entries}
    assert cats == {"hot", "travel", "food"}
