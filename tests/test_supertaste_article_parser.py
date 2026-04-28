from __future__ import annotations

import json
from pathlib import Path

from food_data_ingestion.parsers.supertaste import extract_supertaste_article


FIXTURE = Path(__file__).parent / "fixtures" / "supertaste" / "article_pack_348872.json"


def test_extract_pack_article_metadata_and_candidates():
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))

    extraction = extract_supertaste_article(payload)

    assert extraction.article_id == "348872"
    assert extraction.category == "pack"
    assert extraction.title.startswith("2026台中美食推薦")
    assert extraction.source_url == "https://supertaste.tvbs.com.tw/pack/348872"
    assert extraction.published_at == "2026-04-16"
    assert extraction.image_url and extraction.image_url.startswith("https://")
    assert "台中" in extraction.tags

    # 41 unique stores in this pack roundup
    assert len(extraction.candidates) == 41
    ids = [c.external_id for c in extraction.candidates]
    assert len(set(ids)) == 41

    by_id = {c.external_id: c for c in extraction.candidates}
    sample = by_id["27235"]
    assert sample.name == "大江戶町鰻屋－台中港店"
    assert sample.tags and "美食" in sample.tags
    assert sample.source_url == extraction.source_url

    # 第一張 card：小麥所居酒屋/旬味小肴 公益店 — fixture 裡有 address + phone
    first = by_id["33368"]
    assert first.name.startswith("小麥所居酒屋")
    assert first.address == "台中市南屯區公益路二段37號"
    assert first.phone == "04-23200937"


def test_extract_supports_explicit_source_url_override():
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    extraction = extract_supertaste_article(payload, source_url="https://example/override")
    assert extraction.source_url == "https://example/override"
    # candidate 携帶相同的 source_url
    assert all(c.source_url == "https://example/override" for c in extraction.candidates)


def test_extract_handles_missing_article_content():
    payload = {"data": {"articles_id": "1", "cat_en_name": "food", "title": "x"}}
    extraction = extract_supertaste_article(payload)
    assert extraction.article_id == "1"
    assert extraction.candidates == ()
