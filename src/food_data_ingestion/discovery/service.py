from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from food_data_ingestion.discovery.models import DiscoveredArticle, DiscoveredPlaceCandidate


class CandidateRepositoryProtocol(Protocol):
    def save_discovered_candidates(self, *, article: DiscoveredArticle, candidates: list[DiscoveredPlaceCandidate]) -> list[int]: ...


@dataclass
class UnifiedDiscoveryIngestionService:
    candidate_repository: CandidateRepositoryProtocol

    def ingest_article_candidates(
        self,
        *,
        article: DiscoveredArticle,
        candidates: list[DiscoveredPlaceCandidate],
    ) -> list[int]:
        return self.candidate_repository.save_discovered_candidates(article=article, candidates=candidates)
