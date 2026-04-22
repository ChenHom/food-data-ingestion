from __future__ import annotations

from datetime import datetime, timezone

import pytest

from food_data_ingestion.models.raw_document import RawDocumentCreate, build_content_hash
from food_data_ingestion.storage.raw_repository import RawDocumentRepository


class FakeSession:
    def __init__(self, *, returning_row=None):
        self.returning_row = returning_row or {"id": 0}
        self.execute_returning_calls = []

    def execute_returning(self, query, params):
        self.execute_returning_calls.append((query, params))
        return self.returning_row


def test_build_content_hash_uses_canonical_json_and_ignores_unstable_keys():
    payload_a = {
        "name": "店家",
        "trace_id": "abc",
        "nested": {"request_id": "aaa", "value": 1},
        "items": [{"timestamp": "2026-04-22T00:00:00Z", "id": 1}],
    }
    payload_b = {
        "items": [{"id": 1, "timestamp": "2026-05-01T00:00:00Z"}],
        "nested": {"value": 1, "request_id": "bbb"},
        "name": "店家",
        "trace_id": "xyz",
    }

    assert build_content_hash(raw_json=payload_a) == build_content_hash(raw_json=payload_b)


def test_build_content_hash_falls_back_to_text():
    text = "same content"

    assert build_content_hash(raw_text=text) == build_content_hash(raw_html=text)


def test_raw_document_create_requires_any_raw_content():
    with pytest.raises(ValueError, match="at least one raw payload"):
        RawDocumentCreate(platform="google_places", document_type="place_detail")


def test_create_inserts_raw_document_and_returns_id():
    fetched_at = datetime(2026, 4, 22, 8, 0, tzinfo=timezone.utc)
    observed_at = datetime(2026, 4, 22, 7, 59, tzinfo=timezone.utc)
    parsed_at = datetime(2026, 4, 22, 8, 5, tzinfo=timezone.utc)
    session = FakeSession(returning_row={"id": 42})
    repository = RawDocumentRepository(session)
    payload = RawDocumentCreate(
        crawl_job_id=8,
        source_target_id=3,
        cache_entry_id=5,
        platform="google_places",
        document_type="place_detail",
        source_url="https://maps.google.com/?cid=1",
        canonical_url="https://maps.google.com/?cid=1",
        external_id="ChIJ123",
        parent_external_id=None,
        http_status=200,
        observed_at=observed_at,
        fetched_at=fetched_at,
        raw_json={"name": "店家", "request_id": "ignored"},
        response_headers={"etag": "v1"},
        source_meta={"lang": "zh-TW"},
        parser_version="google_places_v1",
        parsed_at=parsed_at,
    )

    raw_document_id = repository.create(payload)

    assert raw_document_id == 42
    query, params = session.execute_returning_calls[0]
    assert "INSERT INTO ingestion.raw_documents" in query
    assert "RETURNING id" in query
    assert params[0] == 8
    assert params[1] == 3
    assert params[2] == 5
    assert params[3] == "google_places"
    assert params[4] == "place_detail"
    assert params[5] == "https://maps.google.com/?cid=1"
    assert params[6] == "https://maps.google.com/?cid=1"
    assert params[7] == "ChIJ123"
    assert params[9] == 200
    assert params[10] == observed_at
    assert params[11] == fetched_at
    assert params[12] == build_content_hash(raw_json={"name": "店家", "request_id": "ignored"})
    assert params[13] == "pending"
    assert params[14] is None
    assert params[15] is None
    assert params[16] == {"name": "店家", "request_id": "ignored"}
    assert params[17] == {"etag": "v1"}
    assert params[18] == {"lang": "zh-TW"}
    assert params[19] == "google_places_v1"
    assert params[20] == parsed_at
