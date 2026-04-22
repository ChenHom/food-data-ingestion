from __future__ import annotations

import argparse
import json

from food_data_ingestion.config import Settings
from food_data_ingestion.connectors.google_places import GooglePlacesConnector
from food_data_ingestion.db.connection import create_connection
from food_data_ingestion.db.psycopg_session import PsycopgSession
from food_data_ingestion.parsers.google_places import parse_place_detail
from food_data_ingestion.services.ingestion_service import IngestionService
from food_data_ingestion.storage.cache_repository import ApiRequestCacheRepository
from food_data_ingestion.storage.crawl_job_repository import CrawlJobRepository
from food_data_ingestion.storage.raw_repository import RawDocumentRepository
from food_data_ingestion.storage.restaurant_repository import RestaurantRepository


def build_default_service() -> IngestionService:
    settings = Settings.from_env()
    connection = create_connection(settings)
    session = PsycopgSession(connection)
    return IngestionService(
        connector=GooglePlacesConnector(settings=settings, cache_repository=ApiRequestCacheRepository(session)),
        crawl_job_repository=CrawlJobRepository(session),
        cache_repository=ApiRequestCacheRepository(session),
        raw_repository=RawDocumentRepository(session),
        restaurant_repository=RestaurantRepository(session),
        parser=parse_place_detail,
        transaction_manager=session,
    )


def main(argv: list[str] | None = None, *, service: IngestionService | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--place-id", required=True)
    args = parser.parse_args(argv)

    service = service or build_default_service()
    result = service.ingest_google_place_detail(args.place_id)
    print(
        json.dumps(
            {
                "cache_hit": result.cache_hit,
                "job_id": result.job_id,
                "raw_document_id": result.raw_document_id,
                "restaurant_id": result.restaurant_id,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
