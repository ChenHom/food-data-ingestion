from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class DiscoveredArticle:
    source_platform: str
    source_url: str
    title: str
    published_at: str | None
    article_type: str
    categories: tuple[str, ...] = ()
    parser_profile: str = ''
    raw_document_id: int | None = None
    extraction_meta: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DiscoveredPlaceCandidate:
    source_platform: str
    source_url: str
    source_name: str
    candidate_name: str
    address: str | None = None
    phone: str | None = None
    opening_hours: str | None = None
    confidence: float = 0.0
    extraction_method: str = ''
    parser_profile: str = ''
    article_type: str = ''
    raw_document_id: int | None = None
    source_meta: dict[str, Any] = field(default_factory=dict)
