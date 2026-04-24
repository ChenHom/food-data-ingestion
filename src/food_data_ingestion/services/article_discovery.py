from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from food_data_ingestion.discovery.service import UnifiedDiscoveryIngestionService
from food_data_ingestion.models.raw_document import RawDocumentCreate
from food_data_ingestion.parser_profiles.candylife import CandylifeDiscoveryPolicy, CandylifeParserProfile
from food_data_ingestion.parsers.candylife import extract_candylife_article
from food_data_ingestion.parsers.candylife_feed import CandylifeFeedEntry


class HTMLFetcherProtocol(Protocol):
    def fetch_html(self, url: str) -> str: ...


class RawRepositoryProtocol(Protocol):
    def create(self, payload: RawDocumentCreate) -> int: ...


@dataclass(frozen=True)
class ArticleDiscoveryResult:
    raw_document_id: int
    candidate_count: int
    persisted_candidate_ids: list[int]


@dataclass
class CandylifeArticleDiscoveryService:
    fetcher: HTMLFetcherProtocol
    raw_repository: RawRepositoryProtocol
    unified_ingestion_service: UnifiedDiscoveryIngestionService
    policy: CandylifeDiscoveryPolicy = CandylifeDiscoveryPolicy()
    parser_profile: CandylifeParserProfile = CandylifeParserProfile()
    clock: callable = lambda: datetime.now(timezone.utc)

    def discover_article(self, entry: CandylifeFeedEntry) -> ArticleDiscoveryResult:
        html = self.fetcher.fetch_html(entry.link)
        fetched_at = self.clock()
        raw_document_id = self.raw_repository.create(
            RawDocumentCreate(
                platform="candylife",
                document_type="article",
                source_url=entry.link,
                canonical_url=entry.link,
                external_id=entry.link,
                fetched_at=fetched_at,
                raw_html=html,
                source_meta={
                    "title": entry.title,
                    "published_at": entry.published_at,
                    "categories": entry.categories,
                    "article_kind": entry.article_kind.value,
                    "parser_profile": self.parser_profile.name,
                },
            )
        )

        extraction = extract_candylife_article(html=html, source_url=entry.link)
        article = self.parser_profile.to_discovered_article(
            extraction=extraction,
            raw_document_id=raw_document_id,
            article_kind=entry.article_kind,
        )
        if not self.policy.should_extract_candidates(entry.article_kind):
            return ArticleDiscoveryResult(raw_document_id=raw_document_id, candidate_count=0, persisted_candidate_ids=[])

        candidates = self.parser_profile.to_discovered_candidates(
            extraction=extraction,
            raw_document_id=raw_document_id,
            article_kind=entry.article_kind,
        )
        persisted_candidate_ids = self.unified_ingestion_service.ingest_article_candidates(
            article=article,
            candidates=candidates,
        )
        return ArticleDiscoveryResult(
            raw_document_id=raw_document_id,
            candidate_count=len(candidates),
            persisted_candidate_ids=persisted_candidate_ids,
        )
