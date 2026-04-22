from __future__ import annotations

import os

import pytest


def test_google_places_db_smoke_against_real_postgres():
    if os.getenv("RUN_FOOD_DB_SMOKE") != "1":
        pytest.skip("set RUN_FOOD_DB_SMOKE=1 to run real PostgreSQL smoke check")

    from food_data_ingestion.smoke.google_places import run_google_places_db_smoke

    summary = run_google_places_db_smoke(place_id="smoke_pytest_db_001")

    assert summary["first_run"]["cache_hit"] is False
    assert summary["second_run"]["cache_hit"] is True
    assert summary["connector_call_count"] == 1
    assert summary["db_counts"] == {
        "crawl_jobs": 2,
        "api_request_cache": 1,
        "raw_documents": 1,
        "restaurants": 1,
        "restaurant_external_refs": 1,
    }
    assert summary["first_run"]["raw_document_id"] is not None
    assert summary["first_run"]["restaurant_id"] is not None
    assert summary["second_run"]["restaurant_id"] == summary["first_run"]["restaurant_id"]
