from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class ParsedRestaurant:
    canonical_name: str
    normalized_name: str
    address: str | None = None
    latitude: Decimal | None = None
    longitude: Decimal | None = None
    average_rating: Decimal | None = None
    rating_count: int | None = None
    business_hours: dict[str, Any] = field(default_factory=dict)
    source_meta: dict[str, Any] = field(default_factory=dict)
    website: str | None = None
    phone: str | None = None
    price_level: str | None = None
    branch_name: str | None = None
    country: str | None = None
    city: str | None = None
    district: str | None = None
    is_closed: bool = False


@dataclass(frozen=True)
class ParsedExternalRef:
    platform: str
    external_id: str
    external_url: str | None = None
    ref_type: str | None = None
    is_primary: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ParsedPlaceDetail:
    restaurant: ParsedRestaurant
    external_refs: list[ParsedExternalRef]
    aliases: list[str] = field(default_factory=list)
