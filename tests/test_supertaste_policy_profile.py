from __future__ import annotations

from food_data_ingestion.parser_profiles.supertaste import (
    ARTICLE_KIND_ROUNDUP,
    ARTICLE_KIND_SINGLE,
    SupertasteDiscoveryPolicy,
    SupertasteParserProfile,
    classify_article_kind,
)
from food_data_ingestion.parsers.supertaste import (
    SupertasteArticleExtraction,
    SupertasteCandidate,
)
from food_data_ingestion.parsers.supertaste_sitemap import SupertasteSitemapEntry


def _entry(category: str, article_id: str = "1", lastmod: str | None = None) -> SupertasteSitemapEntry:
    return SupertasteSitemapEntry(
        url=f"https://supertaste.tvbs.com.tw/{category}/{article_id}",
        category=category,
        article_id=article_id,
        lastmod=lastmod,
    )


def test_policy_filters_to_allowed_categories_by_default():
    policy = SupertasteDiscoveryPolicy()
    entries = [_entry("pack"), _entry("food"), _entry("hot"), _entry("travel")]
    result = policy.filter_entries(entries)
    assert {e.category for e in result} == {"pack", "food"}


def test_policy_min_lastmod_filters_old_entries():
    policy = SupertasteDiscoveryPolicy(min_lastmod="2025-01-01")
    entries = [
        _entry("pack", "1", "2024-12-31T00:00:00+08:00"),
        _entry("pack", "2", "2025-06-01T00:00:00+08:00"),
        _entry("pack", "3", None),  # missing lastmod is filtered out
    ]
    result = policy.filter_entries(entries)
    assert [e.article_id for e in result] == ["2"]


def test_classify_article_kind_pack_is_roundup_food_default_single():
    assert classify_article_kind(category="pack", title="任意") == ARTICLE_KIND_ROUNDUP
    assert classify_article_kind(category="food", title="某店家專訪") == ARTICLE_KIND_SINGLE


def test_classify_article_kind_food_with_roundup_hint():
    assert classify_article_kind(category="food", title="台中5間燒肉懶人包") == ARTICLE_KIND_ROUNDUP


def test_profile_to_discovered_article_and_candidates():
    extraction = SupertasteArticleExtraction(
        source_url="https://supertaste.tvbs.com.tw/pack/348872",
        article_id="348872",
        category="pack",
        title="title",
        description=None,
        published_at="2026-04-16",
        updated_at=None,
        image_url=None,
        tags=("台中", "美食"),
        candidates=(
            SupertasteCandidate(
                external_id="33368",
                name="店家A",
                address="台中市...",
                phone="04-1234",
                tags=("居酒屋",),
                keywords=("美食",),
                source_url="https://supertaste.tvbs.com.tw/pack/348872",
            ),
        ),
    )
    profile = SupertasteParserProfile()

    article = profile.to_discovered_article(
        extraction=extraction,
        raw_document_id=7,
        article_kind=ARTICLE_KIND_ROUNDUP,
    )
    assert article.source_platform == "supertaste"
    assert article.parser_profile == "supertaste_v1"
    assert article.article_type == "roundup"
    assert article.categories == ("pack",)
    assert article.extraction_meta["article_id"] == "348872"
    assert article.raw_document_id == 7

    candidates = profile.to_discovered_candidates(
        extraction=extraction,
        raw_document_id=7,
        article_kind=ARTICLE_KIND_ROUNDUP,
    )
    assert len(candidates) == 1
    c = candidates[0]
    assert c.source_platform == "supertaste"
    assert c.candidate_name == "店家A"
    assert c.address == "台中市..."
    assert c.phone == "04-1234"
    assert c.extraction_method == "info_card_app"
    assert c.source_meta["external_id"] == "33368"
    assert c.source_meta["tags"] == ["居酒屋"]
    assert c.raw_document_id == 7
