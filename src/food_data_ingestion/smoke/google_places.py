from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import Any

from food_data_ingestion.config import Settings
from food_data_ingestion.connectors.google_places import GooglePlacesConnector, GooglePlacesHttpResponse
from food_data_ingestion.db.connection import create_connection
from food_data_ingestion.db.psycopg_session import PsycopgSession
from food_data_ingestion.jobs.run_google_places_sync import main as run_google_places_sync_main
from food_data_ingestion.parsers.google_places import parse_place_detail
from food_data_ingestion.services.ingestion_service import IngestionService
from food_data_ingestion.storage.cache_repository import ApiRequestCacheRepository, build_cache_key
from food_data_ingestion.storage.crawl_job_repository import CrawlJobRepository
from food_data_ingestion.storage.raw_repository import RawDocumentRepository
from food_data_ingestion.storage.restaurant_repository import RestaurantRepository


@dataclass
class FakeGooglePlacesClient:
    response: GooglePlacesHttpResponse
    calls: list[dict[str, Any]] | None = None

    def __post_init__(self) -> None:
        if self.calls is None:
            self.calls = []

    def fetch_place_detail(self, *, place_id: str, fields: list[str], language: str) -> GooglePlacesHttpResponse:
        self.calls.append(
            {
                "place_id": place_id,
                "fields": list(fields),
                "language": language,
            }
        )
        return self.response


def build_fake_place_detail_response(place_id: str) -> GooglePlacesHttpResponse:
    body = {
        "result": {
            "place_id": place_id,
            "name": "Smoke Test Noodles",
            "formatted_address": "台北市大安區測試路 1 號",
            "geometry": {"location": {"lat": 25.033964, "lng": 121.564468}},
            "rating": 4.6,
            "user_ratings_total": 128,
            "opening_hours": {"weekday_text": ["Monday: 11:00 AM – 9:00 PM"]},
            "website": "https://example.com/smoke-test-noodles",
            "url": f"https://maps.google.com/?cid={place_id}",
            "formatted_phone_number": "02 1234 5678",
        },
        "status": "OK",
    }
    return GooglePlacesHttpResponse(
        status_code=200,
        headers={"content-type": "application/json"},
        json_body=body,
        text_body=json.dumps(body, ensure_ascii=False),
    )


def build_smoke_service(settings: Settings, *, place_id: str):
    connection = create_connection(settings)
    session = PsycopgSession(connection)
    client = FakeGooglePlacesClient(response=build_fake_place_detail_response(place_id))
    cache_repository = ApiRequestCacheRepository(session)
    connector = GooglePlacesConnector(
        settings=settings,
        cache_repository=cache_repository,
        client=client,
    )
    ingestion_service = IngestionService(
        connector=connector,
        crawl_job_repository=CrawlJobRepository(session),
        cache_repository=cache_repository,
        raw_repository=RawDocumentRepository(session),
        restaurant_repository=RestaurantRepository(session),
        parser=parse_place_detail,
        transaction_manager=session,
    )
    return connection, session, client, ingestion_service


def cleanup_smoke_rows(session: PsycopgSession, *, place_id: str) -> None:
    restaurant_ids = session.fetchall(
        """
        SELECT restaurant_id
        FROM ingestion.restaurant_external_refs
        WHERE platform = %s AND external_id = %s
        """,
        ("google_places", place_id),
    )
    ids = [int(row["restaurant_id"]) for row in restaurant_ids]
    if ids:
        session.execute(
            "DELETE FROM ingestion.restaurant_aliases WHERE restaurant_id = ANY(%s)",
            (ids,),
        )
        session.execute(
            "DELETE FROM ingestion.restaurant_external_refs WHERE restaurant_id = ANY(%s)",
            (ids,),
        )
        session.execute(
            "DELETE FROM ingestion.restaurants WHERE id = ANY(%s)",
            (ids,),
        )

    session.execute(
        "DELETE FROM ingestion.raw_documents WHERE platform = %s AND external_id = %s",
        ("google_places", place_id),
    )
    session.execute(
        "DELETE FROM ingestion.crawl_jobs WHERE request_meta ->> %s = %s",
        ("place_id", place_id),
    )
    session.execute(
        "DELETE FROM ingestion.api_request_cache WHERE cache_key = %s",
        (build_cache_key("google_places", "place_detail", place_id),),
    )
    session.commit()


def collect_db_counts(session: PsycopgSession, *, place_id: str) -> dict[str, int]:
    cache_key = build_cache_key("google_places", "place_detail", place_id)
    restaurant_count_row = session.fetchone(
        """
        SELECT COUNT(*) AS count
        FROM ingestion.restaurants r
        WHERE EXISTS (
            SELECT 1
            FROM ingestion.restaurant_external_refs ref
            WHERE ref.restaurant_id = r.id
              AND ref.platform = %s
              AND ref.external_id = %s
        )
        """,
        ("google_places", place_id),
    )
    return {
        "crawl_jobs": int(
            session.fetchone(
                "SELECT COUNT(*) AS count FROM ingestion.crawl_jobs WHERE request_meta ->> %s = %s",
                ("place_id", place_id),
            )["count"]
        ),
        "api_request_cache": int(
            session.fetchone(
                "SELECT COUNT(*) AS count FROM ingestion.api_request_cache WHERE cache_key = %s",
                (cache_key,),
            )["count"]
        ),
        "raw_documents": int(
            session.fetchone(
                "SELECT COUNT(*) AS count FROM ingestion.raw_documents WHERE platform = %s AND external_id = %s",
                ("google_places", place_id),
            )["count"]
        ),
        "restaurants": int(restaurant_count_row["count"]),
        "restaurant_external_refs": int(
            session.fetchone(
                "SELECT COUNT(*) AS count FROM ingestion.restaurant_external_refs WHERE platform = %s AND external_id = %s",
                ("google_places", place_id),
            )["count"]
        ),
    }


def run_google_places_db_smoke(*, place_id: str = "smoke_place_001", settings: Settings | None = None) -> dict[str, Any]:
    settings = settings or Settings.from_env()
    connection, session, client, service = build_smoke_service(settings, place_id=place_id)
    try:
        cleanup_smoke_rows(session, place_id=place_id)

        first_exit_code, first_output = _run_cli_once(service, place_id=place_id)
        second_exit_code, second_output = _run_cli_once(service, place_id=place_id)
        summary = {
            "place_id": place_id,
            "cache_key": build_cache_key("google_places", "place_detail", place_id),
            "first_run": first_output,
            "second_run": second_output,
            "connector_call_count": len(client.calls),
            "db_counts": collect_db_counts(session, place_id=place_id),
        }
        if first_exit_code != 0 or second_exit_code != 0:
            raise RuntimeError(f"smoke CLI failed: first={first_exit_code}, second={second_exit_code}")
        return summary
    finally:
        connection.close()


def _run_cli_once(service: Any, *, place_id: str) -> tuple[int, dict[str, Any]]:
    import contextlib
    import io

    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        exit_code = run_google_places_sync_main(["--place-id", place_id], service=service)
    return exit_code, json.loads(stdout.getvalue().strip())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--place-id", default="smoke_place_001")
    args = parser.parse_args(argv)

    summary = run_google_places_db_smoke(place_id=args.place_id)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
