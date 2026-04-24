from __future__ import annotations

import os
from uuid import uuid4

from food_data_ingestion.config import Settings
from food_data_ingestion.db.connection import create_connection
from food_data_ingestion.db.psycopg_session import PsycopgSession
from food_data_ingestion.jobs.run_candylife_discovery import create_db_backed_repositories


def test_create_db_backed_repositories_returns_real_db_repositories_when_db_smoke_enabled():
    if os.getenv("RUN_FOOD_DB_SMOKE") != "1":
        return

    connection = create_connection(Settings.from_env())
    try:
        raw_repository, candidate_repository = create_db_backed_repositories(connection)
    finally:
        connection.close()

    assert raw_repository.__class__.__name__ == "RawDocumentRepository"
    assert candidate_repository.__class__.__name__ == "DiscoveredPlaceCandidateRepository"


def test_create_db_backed_repositories_uses_session_that_can_write_when_db_smoke_enabled():
    if os.getenv("RUN_FOOD_DB_SMOKE") != "1":
        return

    unique = uuid4().hex[:12]
    connection = create_connection(Settings.from_env())
    try:
        connection.autocommit = False
        raw_repository, _ = create_db_backed_repositories(connection)
        raw_document_id = raw_repository.create(
            payload=__import__("food_data_ingestion.models.raw_document", fromlist=["RawDocumentCreate"]).RawDocumentCreate(
                platform="candylife",
                document_type="article",
                source_url=f"https://example.com/db-factory/{unique}",
                canonical_url=f"https://example.com/db-factory/{unique}",
                external_id=unique,
                fetched_at=__import__("datetime").datetime(2026, 4, 23, 12, 0, tzinfo=__import__("datetime").timezone.utc),
                raw_html="<html></html>",
                source_meta={"scope": "factory_smoke"},
            )
        )
        row = PsycopgSession(connection).fetchone(
            "select id, external_id from ingestion.raw_documents where id = %s",
            (raw_document_id,),
        )
    finally:
        connection.rollback()
        connection.close()

    assert row == {"id": raw_document_id, "external_id": unique}
