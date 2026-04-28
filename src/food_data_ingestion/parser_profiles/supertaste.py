"""Discovery policy + parser profile for supertaste."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable

from food_data_ingestion.discovery.models import DiscoveredArticle, DiscoveredPlaceCandidate
from food_data_ingestion.parsers.supertaste import SupertasteArticleExtraction
from food_data_ingestion.parsers.supertaste_sitemap import SupertasteSitemapEntry


# Supertaste article kinds — kept distinct from candylife's so they evolve
# independently. Stored as plain strings to avoid DB schema coupling.
ARTICLE_KIND_SINGLE = "single_store"
ARTICLE_KIND_ROUNDUP = "roundup"

ROUNDUP_TITLE_HINTS = ("懶人包", "特輯", "推薦清單")
_ROUNDUP_COUNT_RE = re.compile(r"\d+\s*[間家]")


@dataclass(frozen=True)
class SupertasteDiscoveryPolicy:
    allowed_categories: frozenset[str] = frozenset({"pack", "food"})
    min_lastmod: str | None = None  # ISO-8601 lower bound (compared lexicographically)

    def filter_entries(self, entries: Iterable[SupertasteSitemapEntry]) -> list[SupertasteSitemapEntry]:
        result: list[SupertasteSitemapEntry] = []
        for entry in entries:
            if entry.category not in self.allowed_categories:
                continue
            if self.min_lastmod is not None and (entry.lastmod or "") < self.min_lastmod:
                continue
            result.append(entry)
        return result


def classify_article_kind(*, category: str, title: str) -> str:
    """Heuristic: 'pack' is roundup; 'food' default single, override on hint."""
    if category == "pack":
        return ARTICLE_KIND_ROUNDUP
    if any(hint in title for hint in ROUNDUP_TITLE_HINTS):
        return ARTICLE_KIND_ROUNDUP
    if _ROUNDUP_COUNT_RE.search(title):
        # e.g. "5間燒肉" / "10家餐廳"
        return ARTICLE_KIND_ROUNDUP
    return ARTICLE_KIND_SINGLE


@dataclass(frozen=True)
class SupertasteParserProfile:
    name: str = "supertaste_v1"
    source_platform: str = "supertaste"

    def to_discovered_article(
        self,
        *,
        extraction: SupertasteArticleExtraction,
        raw_document_id: int,
        article_kind: str,
    ) -> DiscoveredArticle:
        return DiscoveredArticle(
            source_platform=self.source_platform,
            source_url=extraction.source_url,
            title=extraction.title,
            published_at=extraction.published_at,
            article_type=article_kind,
            categories=(extraction.category,) if extraction.category else (),
            parser_profile=self.name,
            raw_document_id=raw_document_id,
            extraction_meta={
                "article_kind": article_kind,
                "article_id": extraction.article_id,
                "tags": list(extraction.tags),
            },
        )

    def to_discovered_candidates(
        self,
        *,
        extraction: SupertasteArticleExtraction,
        raw_document_id: int,
        article_kind: str,
    ) -> list[DiscoveredPlaceCandidate]:
        candidates: list[DiscoveredPlaceCandidate] = []
        for candidate in extraction.candidates:
            candidates.append(
                DiscoveredPlaceCandidate(
                    source_platform=self.source_platform,
                    source_url=candidate.source_url,
                    source_name=self.source_platform,
                    candidate_name=candidate.name,
                    address=candidate.address,
                    phone=candidate.phone,
                    opening_hours=None,
                    confidence=0.95,
                    extraction_method="info_card_app",
                    parser_profile=self.name,
                    article_type=article_kind,
                    raw_document_id=raw_document_id,
                    source_meta={
                        "external_id": candidate.external_id,
                        "tags": list(candidate.tags),
                        "keywords": list(candidate.keywords),
                    },
                )
            )
        return candidates
