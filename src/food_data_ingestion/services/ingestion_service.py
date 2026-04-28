"""Backward-compatible facade.

Implementation now lives in `services.ingestion_context` (orchestration
primitives) and `services.google_places_ingestion` (per-source flow). This
module keeps the old constructor + method signature so existing callers and
tests don't have to change.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

from food_data_ingestion.models.parser_input import ParserInput
from food_data_ingestion.services.google_places_ingestion import (
    ConnectorProtocol,
    GooglePlacesIngestion,
    IngestionResult,
    RestaurantRepositoryProtocol,
    SourceTargetRepositoryProtocol,
)
from food_data_ingestion.services.ingestion_context import (
    AdvisoryLockManagerProtocol,
    CrawlJobRepositoryProtocol,
    IngestionContext,
    RawRepositoryProtocol,
    TransactionManagerProtocol,
)

__all__ = [
    "AdvisoryLockManagerProtocol",
    "ConnectorProtocol",
    "CrawlJobRepositoryProtocol",
    "IngestionResult",
    "IngestionService",
    "RawRepositoryProtocol",
    "RestaurantRepositoryProtocol",
    "SourceTargetRepositoryProtocol",
    "TransactionManagerProtocol",
]


class IngestionService:
    def __init__(
        self,
        *,
        connector: ConnectorProtocol,
        crawl_job_repository: CrawlJobRepositoryProtocol,
        raw_repository: RawRepositoryProtocol,
        restaurant_repository: RestaurantRepositoryProtocol,
        parser: Callable[[ParserInput], Any],
        now_provider: Callable[[], datetime] | None = None,
        transaction_manager: TransactionManagerProtocol | None = None,
        source_target_repository: SourceTargetRepositoryProtocol | None = None,
        advisory_lock_manager: AdvisoryLockManagerProtocol | None = None,
    ) -> None:
        self._ctx = IngestionContext(
            crawl_job_repository=crawl_job_repository,
            raw_repository=raw_repository,
            transaction_manager=transaction_manager,
            advisory_lock_manager=advisory_lock_manager,
            now_provider=now_provider,
        )
        self._google_places = GooglePlacesIngestion(
            ctx=self._ctx,
            connector=connector,
            restaurant_repository=restaurant_repository,
            parser=parser,
            source_target_repository=source_target_repository,
        )

    def ingest_google_place_detail(
        self,
        place_id: str,
        *,
        source_target_id: int | None = None,
        crawl_policy: dict[str, Any] | None = None,
    ) -> IngestionResult:
        return self._google_places.ingest(
            place_id,
            source_target_id=source_target_id,
            crawl_policy=crawl_policy,
        )
