from __future__ import annotations

from dataclasses import dataclass

from food_data_ingestion.discovery.models import DiscoveredArticle, DiscoveredPlaceCandidate
from food_data_ingestion.parsers.candylife import CandylifeArticleExtraction
from food_data_ingestion.parsers.candylife_feed import ArticleKind, CandylifeFeedEntry


@dataclass(frozen=True)
class CandylifeDiscoveryPolicy:
    min_year: int = 2025
    extract_roundup_candidates: bool = False

    def should_process_entry(self, entry: CandylifeFeedEntry) -> bool:
        return entry.published_year >= self.min_year

    def should_extract_candidates(self, article_kind: ArticleKind) -> bool:
        if article_kind is ArticleKind.ROUNDUP:
            return self.extract_roundup_candidates
        return True


@dataclass(frozen=True)
class CandylifeParserProfile:
    name: str = 'candylife_v1'
    source_platform: str = 'candylife'

    def to_discovered_article(
        self,
        *,
        extraction: CandylifeArticleExtraction,
        raw_document_id: int,
        article_kind: ArticleKind,
    ) -> DiscoveredArticle:
        return DiscoveredArticle(
            source_platform=self.source_platform,
            source_url=extraction.source_url,
            title=extraction.title,
            published_at=extraction.published_at,
            article_type=article_kind.value,
            categories=extraction.categories,
            parser_profile=self.name,
            raw_document_id=raw_document_id,
            extraction_meta={'article_kind': article_kind.value},
        )

    def to_discovered_candidates(
        self,
        *,
        extraction: CandylifeArticleExtraction,
        raw_document_id: int,
        article_kind: ArticleKind,
    ) -> list[DiscoveredPlaceCandidate]:
        candidates: list[DiscoveredPlaceCandidate] = []
        for candidate in extraction.restaurant_candidates:
            candidates.append(
                DiscoveredPlaceCandidate(
                    source_platform=self.source_platform,
                    source_url=candidate.source_url,
                    source_name=self.source_platform,
                    candidate_name=candidate.name,
                    address=candidate.address,
                    phone=candidate.phone,
                    opening_hours=candidate.opening_hours,
                    confidence=0.95,
                    extraction_method='store_info_block',
                    parser_profile=self.name,
                    article_type=article_kind.value,
                    raw_document_id=raw_document_id,
                )
            )
        return candidates
