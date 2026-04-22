from __future__ import annotations

import json

from food_data_ingestion.jobs.run_google_places_sync import main
from food_data_ingestion.services.ingestion_service import IngestionResult


class FakeService:
    def __init__(self):
        self.calls = []

    def ingest_google_place_detail(self, place_id: str, *, source_target_id: int | None = None):
        self.calls.append((place_id, source_target_id))
        return IngestionResult(cache_hit=True, job_id=1, raw_document_id=None, restaurant_id=99)



def test_cli_calls_service_and_prints_result(capsys):
    service = FakeService()

    exit_code = main(["--place-id", "ChIJ123"], service=service)

    assert exit_code == 0
    assert service.calls == [("ChIJ123", None)]
    output = capsys.readouterr().out.strip()
    assert json.loads(output) == {
        "cache_hit": True,
        "job_id": 1,
        "raw_document_id": None,
        "restaurant_id": 99,
    }



def test_cli_passes_source_target_id_to_service(capsys):
    service = FakeService()

    exit_code = main(["--place-id", "ChIJ123", "--source-target-id", "42"], service=service)

    assert exit_code == 0
    assert service.calls == [("ChIJ123", 42)]
    output = capsys.readouterr().out.strip()
    assert json.loads(output)["job_id"] == 1
