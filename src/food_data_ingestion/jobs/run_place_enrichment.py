"""CLI: 從 discovered_place_candidates(pending) 跑 enrichment。"""

from __future__ import annotations

import argparse
import json

from food_data_ingestion.config import Settings
from food_data_ingestion.connectors.google_places import GooglePlacesConnector
from food_data_ingestion.db.connection import create_connection
from food_data_ingestion.db.psycopg_session import PsycopgSession
from food_data_ingestion.services.place_enrichment import PlaceEnrichmentService
from food_data_ingestion.storage.cache_repository import ApiRequestCacheRepository
from food_data_ingestion.storage.discovered_candidate_repository import (
    DiscoveredPlaceCandidateRepository,
)
from food_data_ingestion.storage.restaurant_repository import RestaurantRepository


def build_default_service():
    settings = Settings.from_env()
    connection = create_connection(settings)
    session = PsycopgSession(connection)
    cache_repo = ApiRequestCacheRepository(session)
    service = PlaceEnrichmentService(
        candidate_repository=DiscoveredPlaceCandidateRepository(session),
        connector=GooglePlacesConnector(settings=settings, cache_repository=cache_repo),
        restaurant_repository=RestaurantRepository(session),
        transaction_manager=session,
    )
    return service, connection


def main(
    argv: list[str] | None = None,
    *,
    service: PlaceEnrichmentService | None = None,
) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    owns_service = service is None
    connection = None
    if owns_service:
        service, connection = build_default_service()

    try:
        report = service.enrich_pending(limit=args.limit, dry_run=args.dry_run)
    finally:
        if owns_service and connection is not None:
            connection.close()

    print(
        json.dumps(
            {
                "dry_run": args.dry_run,
                "processed": report.processed,
                "matched": report.matched,
                "ambiguous": report.ambiguous,
                "no_match": report.no_match,
                "failed": report.failed,
                "cache_hits": report.cache_hits,
                "outcomes": [
                    {
                        "candidate_id": o.candidate_id,
                        "match_status": o.match_status,
                        "matched_place_id": o.matched_place_id,
                        "matched_restaurant_id": o.matched_restaurant_id,
                        "cache_hit": o.cache_hit,
                        "hit_count": o.hit_count,
                        "reason": o.reason,
                    }
                    for o in report.outcomes
                ],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
