from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Protocol

from food_data_ingestion.db.json import as_jsonb
from food_data_ingestion.discovery.models import DiscoveredArticle, DiscoveredPlaceCandidate


@dataclass(frozen=True)
class PendingCandidate:
    id: int
    source_platform: str
    source_url: str
    source_name: str
    candidate_name: str
    address: str | None
    phone: str | None
    opening_hours: str | None
    article_type: str | None
    parser_profile: str | None
    raw_document_id: int | None
    match_attempt_count: int
    source_meta: dict[str, Any]


class SessionProtocol(Protocol):
    def execute_returning(self, query: str, params: tuple[Any, ...]) -> dict[str, Any]: ...

    def fetchall(self, query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]: ...

    def execute(self, query: str, params: tuple[Any, ...]) -> None: ...


def build_candidate_key(*, source_platform: str, source_url: str, candidate_name: str, raw_document_id: int | None) -> str:
    raw_part = "" if raw_document_id is None else str(raw_document_id)
    return "|".join((source_platform.strip().lower(), source_url.strip(), candidate_name.strip().lower(), raw_part))


class DiscoveredPlaceCandidateRepository:
    def __init__(self, session: SessionProtocol):
        self.session = session

    def save_discovered_candidates(
        self,
        *,
        article: DiscoveredArticle,
        candidates: list[DiscoveredPlaceCandidate],
    ) -> list[int]:
        inserted_ids: list[int] = []
        article_payload = asdict(article)
        for candidate in candidates:
            candidate_payload = asdict(candidate)
            metadata = {
                "article": article_payload,
                "candidate": {
                    **candidate_payload,
                    "categories": list(article.categories),
                },
            }
            row = self.session.execute_returning(
                """
                INSERT INTO ingestion.discovered_place_candidates (
                    source_platform,
                    source_url,
                    source_name,
                    candidate_name,
                    address,
                    phone,
                    opening_hours,
                    confidence_score,
                    extraction_method,
                    parser_profile,
                    article_type,
                    raw_document_id,
                    source_meta,
                    candidate_key
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (candidate_key) DO UPDATE
                SET source_name = EXCLUDED.source_name,
                    address = EXCLUDED.address,
                    phone = EXCLUDED.phone,
                    opening_hours = EXCLUDED.opening_hours,
                    confidence_score = EXCLUDED.confidence_score,
                    extraction_method = EXCLUDED.extraction_method,
                    parser_profile = EXCLUDED.parser_profile,
                    article_type = EXCLUDED.article_type,
                    raw_document_id = EXCLUDED.raw_document_id,
                    source_meta = EXCLUDED.source_meta,
                    updated_at = NOW()
                RETURNING id
                """,
                (
                    candidate.source_platform,
                    candidate.source_url,
                    candidate.source_name,
                    candidate.candidate_name,
                    candidate.address,
                    candidate.phone,
                    candidate.opening_hours,
                    candidate.confidence,
                    candidate.extraction_method,
                    candidate.parser_profile,
                    candidate.article_type,
                    candidate.raw_document_id,
                    as_jsonb(metadata),
                    build_candidate_key(
                        source_platform=candidate.source_platform,
                        source_url=candidate.source_url,
                        candidate_name=candidate.candidate_name,
                        raw_document_id=candidate.raw_document_id,
                    ),
                ),
            )
            inserted_ids.append(int(row["id"]))
        return inserted_ids

    def list_pending_for_match(self, *, limit: int) -> list[PendingCandidate]:
        rows = self.session.fetchall(
            """
            SELECT id, source_platform, source_url, source_name, candidate_name,
                   address, phone, opening_hours, article_type, parser_profile,
                   raw_document_id, match_attempt_count, source_meta
            FROM ingestion.discovered_place_candidates
            WHERE match_status = 'pending'
            ORDER BY match_attempt_count ASC, id ASC
            LIMIT %s
            """,
            (limit,),
        )
        return [
            PendingCandidate(
                id=int(r["id"]),
                source_platform=r["source_platform"],
                source_url=r["source_url"],
                source_name=r["source_name"],
                candidate_name=r["candidate_name"],
                address=r["address"],
                phone=r["phone"],
                opening_hours=r["opening_hours"],
                article_type=r["article_type"],
                parser_profile=r["parser_profile"],
                raw_document_id=int(r["raw_document_id"]) if r["raw_document_id"] is not None else None,
                match_attempt_count=int(r["match_attempt_count"] or 0),
                source_meta=r["source_meta"] or {},
            )
            for r in rows
        ]

    def apply_match_result(
        self,
        *,
        candidate_id: int,
        match_status: str,
        matched_place_id: str | None,
        matched_restaurant_id: int | None,
        match_meta: dict[str, Any],
        attempt_at: datetime,
    ) -> None:
        self.session.execute(
            """
            UPDATE ingestion.discovered_place_candidates
            SET match_status = %s,
                matched_place_id = %s,
                matched_restaurant_id = %s,
                match_meta = %s,
                last_match_attempt_at = %s,
                match_attempt_count = match_attempt_count + 1,
                updated_at = NOW()
            WHERE id = %s
            """,
            (
                match_status,
                matched_place_id,
                matched_restaurant_id,
                as_jsonb(match_meta),
                attempt_at,
                candidate_id,
            ),
        )
