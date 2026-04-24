from __future__ import annotations

from food_data_ingestion.discovery.models import DiscoveredArticle, DiscoveredPlaceCandidate
from food_data_ingestion.parser_profiles.candylife import CandylifeDiscoveryPolicy, CandylifeParserProfile
from food_data_ingestion.parsers.candylife import extract_candylife_article
from food_data_ingestion.parsers.candylife_feed import ArticleKind, CandylifeFeedEntry


def test_candylife_discovery_policy_keeps_2025_plus_and_allows_roundup_skip():
    policy = CandylifeDiscoveryPolicy()
    new_entry = CandylifeFeedEntry(
        title='阿發現炒｜單店',
        link='https://candylife.tw/a/',
        published_at='Thu, 23 Apr 2026 06:24:32 +0000',
        categories=('台中美食',),
        article_kind=ArticleKind.SINGLE_STORE,
    )
    old_entry = CandylifeFeedEntry(
        title='2024 舊文章',
        link='https://candylife.tw/old/',
        published_at='Mon, 20 May 2024 13:23:56 +0000',
        categories=('台中美食',),
        article_kind=ArticleKind.SINGLE_STORE,
    )

    assert policy.should_process_entry(new_entry) is True
    assert policy.should_process_entry(old_entry) is False
    assert policy.should_extract_candidates(ArticleKind.SINGLE_STORE) is True
    assert policy.should_extract_candidates(ArticleKind.ROUNDUP) is False


def test_candylife_parser_profile_normalizes_article_and_candidates():
    html = '''<html><head><title>255 LAB café｜台中南屯咖啡廳推薦 - 糖糖\'s 享食生活</title><meta property="article:published_time" content="2026-04-21T11:45:48+00:00" /></head><body><article><h1>255 LAB café｜台中南屯咖啡廳推薦</h1><a href="https://candylife.tw/category/taichung-food/">台中美食</a><p>《店家資訊》</p><p>店家：255 LAB café 電話：04-22512075 地址：台中市南屯區大墩十一街392號 時間：平日08:00~16:00；假日10:00~18:00</p></article></body></html>'''
    extraction = extract_candylife_article(html=html, source_url='https://candylife.tw/255labcafe/')
    profile = CandylifeParserProfile()

    article = profile.to_discovered_article(extraction=extraction, raw_document_id=55, article_kind=ArticleKind.SINGLE_STORE)
    candidates = profile.to_discovered_candidates(extraction=extraction, raw_document_id=55, article_kind=ArticleKind.SINGLE_STORE)

    assert article.source_platform == 'candylife'
    assert article.parser_profile == 'candylife_v1'
    assert article.article_type == 'single_store'
    assert candidates[0].source_platform == 'candylife'
    assert candidates[0].parser_profile == 'candylife_v1'
    assert candidates[0].raw_document_id == 55
    assert candidates[0].candidate_name == '255 LAB café'
