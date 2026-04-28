from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from food_data_ingestion.services.place_enrichment import (
    PlaceEnrichmentService,
    build_search_query,
    decide_match,
)
from food_data_ingestion.storage.discovered_candidate_repository import PendingCandidate


def make_candidate(
    *,
    candidate_id: int = 1,
    name: str = "鼎泰豐 信義店",
    address: str | None = "台北市信義區市府路45號",
) -> PendingCandidate:
    return PendingCandidate(
        id=candidate_id,
        source_platform="supertaste",
        source_url=f"https://example.com/{candidate_id}",
        source_name="supertaste",
        candidate_name=name,
        address=address,
        phone=None,
        opening_hours=None,
        article_type=None,
        parser_profile=None,
        raw_document_id=None,
        match_attempt_count=0,
        source_meta={},
    )


def make_hit(place_id: str, name: str, address: str = "", types: tuple[str, ...] = ("restaurant",)):
    from food_data_ingestion.parsers.google_places import PlaceSearchHit

    return PlaceSearchHit(
        place_id=place_id,
        display_name=name,
        formatted_address=address,
        latitude=None,
        longitude=None,
        types=types,
    )


class FakeCandidateRepo:
    def __init__(self, pending: list[PendingCandidate]) -> None:
        self._pending = pending
        self.applied: list[dict[str, Any]] = []

    def list_pending_for_match(self, *, limit: int) -> list[PendingCandidate]:
        return self._pending[:limit]

    def apply_match_result(self, **kwargs: Any) -> None:
        self.applied.append(kwargs)


class FakeRestaurantRepo:
    def __init__(self) -> None:
        self.upserts: list[Any] = []

    def upsert(self, parsed: Any) -> int:
        self.upserts.append(parsed)
        return 100 + len(self.upserts)


class FakeConnector:
    def __init__(self, results: list[dict[str, Any]]) -> None:
        self._results = list(results)
        self.calls: list[str] = []

    def search_text(self, text_query: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(text_query)
        return self._results.pop(0)


class FakeTxn:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def _fetch_result(hits: list, *, cache_hit: bool = False, error: str | None = None) -> dict[str, Any]:
    body = {
        "places": [
            {
                "id": h.place_id,
                "displayName": {"text": h.display_name},
                "formattedAddress": h.formatted_address,
                "types": list(h.types),
            }
            for h in hits
        ]
    }
    return {
        "response_body": body,
        "is_error": error is not None,
        "error_message": error,
        "source_meta": {"cache_hit": cache_hit},
    }


# ---------------- decide_match ----------------


def test_decide_match_no_hits_returns_no_match():
    status, idx, _ = decide_match(make_candidate(), [])
    assert status == "no_match"
    assert idx is None


def test_decide_match_single_hit_returns_matched():
    status, idx, _ = decide_match(make_candidate(), [make_hit("p1", "鼎泰豐 信義店")])
    assert status == "matched"
    assert idx == 0


def test_decide_match_multiple_hits_with_address_overlap_picks_top():
    cand = make_candidate(name="鼎泰豐", address="台北市 信義區 市府路")
    hits = [
        make_hit("p1", "鼎泰豐", "台北市信義區市府路 45號"),
        make_hit("p2", "鼎泰豐 內湖店", "台北市內湖區瑞光路"),
    ]
    status, idx, _ = decide_match(cand, hits)
    assert status == "matched"
    assert idx == 0


def test_decide_match_multiple_hits_name_mismatch_returns_ambiguous():
    cand = make_candidate(name="鼎泰豐")
    hits = [make_hit("p1", "添好運", "addr"), make_hit("p2", "蘇杭", "addr2")]
    status, idx, _ = decide_match(cand, hits)
    assert status == "ambiguous"
    assert idx is None


def test_decide_match_multiple_hits_no_address_returns_ambiguous():
    cand = make_candidate(name="鼎泰豐", address=None)
    hits = [make_hit("p1", "鼎泰豐", "addr1"), make_hit("p2", "鼎泰豐", "addr2")]
    status, _, _ = decide_match(cand, hits)
    assert status == "ambiguous"


# ---------------- build_search_query ----------------


def test_build_search_query_includes_address():
    q = build_search_query(make_candidate(name=" 鼎泰豐 ", address=" 台北 "))
    assert q == "鼎泰豐 台北"


def test_build_search_query_handles_missing_address():
    q = build_search_query(make_candidate(address=None))
    assert q == "鼎泰豐 信義店"


# ---------------- PlaceEnrichmentService ----------------


def test_enrich_pending_matched_writes_restaurant_and_applies_result():
    cand = make_candidate()
    hit = make_hit("places/abc", "鼎泰豐 信義店", cand.address or "")
    repo = FakeCandidateRepo([cand])
    rest = FakeRestaurantRepo()
    txn = FakeTxn()
    svc = PlaceEnrichmentService(
        candidate_repository=repo,
        connector=FakeConnector([_fetch_result([hit])]),
        restaurant_repository=rest,
        transaction_manager=txn,
        now_provider=lambda: datetime(2026, 4, 22, tzinfo=UTC),
    )

    report = svc.enrich_pending(limit=10)

    assert report.processed == 1
    assert report.matched == 1
    assert len(rest.upserts) == 1
    assert repo.applied[0]["match_status"] == "matched"
    assert repo.applied[0]["matched_place_id"] == "places/abc"
    assert repo.applied[0]["matched_restaurant_id"] == 101
    assert txn.commits == 1


def test_enrich_pending_no_match_skips_restaurant_upsert():
    repo = FakeCandidateRepo([make_candidate()])
    rest = FakeRestaurantRepo()
    svc = PlaceEnrichmentService(
        candidate_repository=repo,
        connector=FakeConnector([_fetch_result([])]),
        restaurant_repository=rest,
        transaction_manager=FakeTxn(),
    )

    report = svc.enrich_pending(limit=10)
    assert report.no_match == 1
    assert rest.upserts == []
    assert repo.applied[0]["match_status"] == "no_match"


def test_enrich_pending_dry_run_does_not_write():
    cand = make_candidate()
    hit = make_hit("places/abc", cand.candidate_name, cand.address or "")
    repo = FakeCandidateRepo([cand])
    rest = FakeRestaurantRepo()
    txn = FakeTxn()
    svc = PlaceEnrichmentService(
        candidate_repository=repo,
        connector=FakeConnector([_fetch_result([hit])]),
        restaurant_repository=rest,
        transaction_manager=txn,
    )

    report = svc.enrich_pending(limit=10, dry_run=True)
    assert report.matched == 1
    assert rest.upserts == []
    assert repo.applied == []
    assert txn.commits == 0


def test_enrich_pending_connector_error_marks_failed():
    repo = FakeCandidateRepo([make_candidate()])

    class BoomConnector:
        def search_text(self, text_query: str, **_: Any) -> dict[str, Any]:
            raise RuntimeError("boom")

    svc = PlaceEnrichmentService(
        candidate_repository=repo,
        connector=BoomConnector(),
        restaurant_repository=FakeRestaurantRepo(),
    )
    report = svc.enrich_pending(limit=10)
    assert report.failed == 1
    assert repo.applied[0]["match_status"] == "failed"


def test_enrich_pending_search_error_marks_failed():
    repo = FakeCandidateRepo([make_candidate()])
    svc = PlaceEnrichmentService(
        candidate_repository=repo,
        connector=FakeConnector([_fetch_result([], error="quota exceeded")]),
        restaurant_repository=FakeRestaurantRepo(),
    )
    report = svc.enrich_pending(limit=10)
    assert report.failed == 1
    assert "quota exceeded" in repo.applied[0]["match_meta"]["reason"]


def test_enrich_pending_records_cache_hit_flag():
    cand = make_candidate()
    hit = make_hit("places/abc", cand.candidate_name, cand.address or "")
    repo = FakeCandidateRepo([cand])
    svc = PlaceEnrichmentService(
        candidate_repository=repo,
        connector=FakeConnector([_fetch_result([hit], cache_hit=True)]),
        restaurant_repository=FakeRestaurantRepo(),
    )
    report = svc.enrich_pending(limit=10)
    assert report.cache_hits == 1
    assert repo.applied[0]["match_meta"]["cache_hit"] is True
