"""GooglePlacesIngestion: per-source flow that composes IngestionContext primitives."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Protocol

from food_data_ingestion.connectors.base import FetchResult
from food_data_ingestion.models.parser_input import ParserInput
from food_data_ingestion.services.ingestion_context import IngestionContext


@dataclass(frozen=True)
class IngestionResult:
    cache_hit: bool
    job_id: int
    raw_document_id: int | None
    restaurant_id: int | None


class ConnectorProtocol(Protocol):
    def fetch_place_detail(
        self,
        place_id: str,
        *,
        fields: list[str] | None = None,
        language: str = "zh-TW",
        crawl_policy: dict[str, Any] | None = None,
    ) -> FetchResult: ...


class RestaurantRepositoryProtocol(Protocol):
    def upsert(self, parsed: Any) -> int: ...


class SourceTargetRepositoryProtocol(Protocol):
    def get_crawl_policy(self, source_target_id: int) -> dict[str, Any]: ...


class GooglePlacesIngestion:
    PLATFORM = "google_places"
    JOB_TYPE = "place_detail"

    def __init__(
        self,
        *,
        ctx: IngestionContext,
        connector: ConnectorProtocol,
        restaurant_repository: RestaurantRepositoryProtocol,
        parser: Callable[[ParserInput], Any],
        source_target_repository: SourceTargetRepositoryProtocol | None = None,
    ) -> None:
        self.ctx = ctx
        self.connector = connector
        self.restaurant_repository = restaurant_repository
        self.parser = parser
        self.source_target_repository = source_target_repository

    def ingest(
        self,
        place_id: str,
        *,
        source_target_id: int | None = None,
        crawl_policy: dict[str, Any] | None = None,
    ) -> IngestionResult:
        effective_policy = self._resolve_crawl_policy(
            source_target_id=source_target_id,
            crawl_policy=crawl_policy,
        )
        request_meta: dict[str, Any] = {"place_id": place_id}
        if source_target_id is not None:
            request_meta["source_target_id"] = source_target_id

        with self.ctx.crawl_session(
            platform=self.PLATFORM,
            job_type=self.JOB_TYPE,
            identifier=place_id,
            source_target_id=source_target_id,
            request_meta=request_meta,
        ) as session:
            fetch_result = self.connector.fetch_place_detail(place_id, crawl_policy=effective_policy)
            cache_hit = bool(fetch_result.get("source_meta", {}).get("cache_hit"))

            raw_document_id = self.ctx.store_raw_from_fetch(
                fetch_result,
                crawl_job_id=session.job_id,
                source_target_id=source_target_id,
                external_id=place_id,
            )

            session.failure_stats = self._build_failure_stats(
                cache_hit=cache_hit,
                raw_document_id=raw_document_id,
                source_target_id=source_target_id,
            )

            parser_input = ParserInput.from_fetch_result(fetch_result, external_id=place_id)
            parsed = self.parser(parser_input)
            restaurant_id = self.restaurant_repository.upsert(parsed)
            if self.ctx.transaction_manager is not None:
                self.ctx.transaction_manager.commit()

            session.success_stats = self._build_success_stats(
                cache_hit=cache_hit,
                raw_document_id=raw_document_id,
                source_target_id=source_target_id,
            )

        return IngestionResult(
            cache_hit=cache_hit,
            job_id=session.job_id,
            raw_document_id=raw_document_id,
            restaurant_id=restaurant_id,
        )

    def _resolve_crawl_policy(
        self,
        *,
        source_target_id: int | None,
        crawl_policy: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        resolved: dict[str, Any] = {}
        if source_target_id is not None and self.source_target_repository is not None:
            resolved.update(self.source_target_repository.get_crawl_policy(source_target_id))
        if crawl_policy:
            resolved.update(crawl_policy)
        return resolved or None

    @staticmethod
    def _build_success_stats(
        *,
        cache_hit: bool,
        raw_document_id: int | None,
        source_target_id: int | None,
    ) -> dict[str, Any]:
        stats: dict[str, Any] = {
            "cache_hit": cache_hit,
            "raw_document_id": raw_document_id,
            "restaurant_id_count": 1,
            "content_count": 0,
        }
        if source_target_id is not None:
            stats["source_target_id"] = source_target_id
        return stats

    @staticmethod
    def _build_failure_stats(
        *,
        cache_hit: bool,
        raw_document_id: int | None,
        source_target_id: int | None,
    ) -> dict[str, Any]:
        stats: dict[str, Any] = {
            "cache_hit": cache_hit,
            "raw_document_id": raw_document_id,
            "content_count": 0,
        }
        if source_target_id is not None:
            stats["source_target_id"] = source_target_id
        return stats
