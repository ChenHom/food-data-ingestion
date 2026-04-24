from __future__ import annotations

from dataclasses import asdict
from typing import Any, Protocol

from food_data_ingestion.db.json import as_jsonb
from food_data_ingestion.discovery.models import DiscoveredArticle, DiscoveredPlaceCandidate


class SessionProtocol(Protocol):
    def execute_returning(self, query: str, params: tuple[Any, ...]) -> dict[str, Any]: ...


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
