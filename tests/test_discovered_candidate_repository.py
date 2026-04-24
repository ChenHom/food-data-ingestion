from __future__ import annotations

from pathlib import Path

from food_data_ingestion.discovery.models import DiscoveredArticle, DiscoveredPlaceCandidate
from food_data_ingestion.storage.discovered_candidate_repository import DiscoveredPlaceCandidateRepository


class FakeSession:
    def __init__(self):
        self.calls = []
        self.next_id = 500

    def execute_returning(self, query, params):
        self.calls.append((query, params))
        current = self.next_id
        self.next_id += 1
        return {"id": current}


def build_article() -> DiscoveredArticle:
    return DiscoveredArticle(
        source_platform="candylife",
        source_url="https://candylife.tw/255labcafe/",
        title="255 LAB café｜台中南屯咖啡廳推薦",
        published_at="2026-04-21T11:45:48+00:00",
        article_type="single_store",
        categories=("台中美食", "台中咖啡"),
        parser_profile="candylife_v1",
        raw_document_id=101,
        extraction_meta={"article_kind": "single_store"},
    )


def build_candidate() -> DiscoveredPlaceCandidate:
    return DiscoveredPlaceCandidate(
        source_platform="candylife",
        source_url="https://candylife.tw/255labcafe/",
        source_name="candylife",
        candidate_name="255 LAB café",
        address="台中市南屯區大墩十一街392號",
        phone="04-22512075",
        opening_hours="平日08:00~16:00；假日10:00~18:00",
        confidence=0.95,
        extraction_method="store_info_block",
        parser_profile="candylife_v1",
        article_type="single_store",
        raw_document_id=101,
        source_meta={"categories": ["台中美食", "台中咖啡"]},
    )


def test_save_discovered_candidates_inserts_rows_and_returns_ids():
    session = FakeSession()
    repo = DiscoveredPlaceCandidateRepository(session)

    ids = repo.save_discovered_candidates(article=build_article(), candidates=[build_candidate()])

    assert ids == [500]
    query, params = session.calls[0]
    assert "INSERT INTO ingestion.discovered_place_candidates" in query
    assert params[0] == "candylife"
    assert params[1] == "https://candylife.tw/255labcafe/"
    assert params[2] == "candylife"
    assert params[3] == "255 LAB café"
    assert params[4] == "台中市南屯區大墩十一街392號"
    assert params[7] == 0.95
    assert params[9] == "candylife_v1"
    assert params[10] == "single_store"
    assert params[11] == 101
    payload = getattr(params[12], "obj", params[12])
    assert payload["article"]["title"] == "255 LAB café｜台中南屯咖啡廳推薦"
    assert payload["candidate"]["categories"] == ["台中美食", "台中咖啡"]


def test_discovered_place_candidates_migration_declares_staging_table():
    migration = Path("database/migrations/004_add_discovered_place_candidates.sql").read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS ingestion.discovered_place_candidates" in migration
    assert "candidate_name" in migration
    assert "raw_document_id" in migration
    assert "confidence_score" in migration
    assert "candidate_key" in migration
