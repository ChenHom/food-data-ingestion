from __future__ import annotations

from typing import Any, Protocol

from food_data_ingestion.db.json import as_jsonb
from food_data_ingestion.models.raw_document import RawDocumentCreate


class SessionProtocol(Protocol):
    def execute_returning(self, query: str, params: tuple[Any, ...]) -> dict[str, Any] | None: ...


class RawDocumentRepository:
    def __init__(self, session: SessionProtocol):
        self.session = session

    def create(self, payload: RawDocumentCreate) -> int:
        row = self.session.execute_returning(
            """
            INSERT INTO ingestion.raw_documents (
                crawl_job_id,
                source_target_id,
                cache_entry_id,
                platform,
                document_type,
                source_url,
                canonical_url,
                external_id,
                parent_external_id,
                http_status,
                observed_at,
                fetched_at,
                content_hash,
                parse_status,
                raw_html,
                raw_text,
                raw_json,
                response_headers,
                source_meta,
                parser_version,
                parsed_at
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s
            )
            RETURNING id
            """,
            (
                payload.crawl_job_id,
                payload.source_target_id,
                payload.cache_entry_id,
                payload.platform,
                payload.document_type,
                payload.source_url,
                payload.canonical_url,
                payload.external_id,
                payload.parent_external_id,
                payload.http_status,
                payload.observed_at,
                payload.fetched_at,
                payload.content_hash,
                payload.parse_status,
                payload.raw_html,
                payload.raw_text,
                as_jsonb(payload.raw_json) if payload.raw_json is not None else None,
                as_jsonb(payload.response_headers) if payload.response_headers is not None else None,
                as_jsonb(payload.source_meta),
                payload.parser_version,
                payload.parsed_at,
            ),
        )
        if not row or "id" not in row:
            raise RuntimeError("failed to create raw document")
        return int(row["id"])
