"""Places API (New) response → ParsedPlaceDetail / list[PlaceSearchHit]。"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
import re
from typing import Any

from food_data_ingestion.models.parser_input import ParserInput
from food_data_ingestion.models.restaurant import (
    ParsedExternalRef,
    ParsedPlaceDetail,
    ParsedRestaurant,
)

_PAREN_CONTENT_RE = re.compile(r"\s*\([^)]*\)")
_MULTISPACE_RE = re.compile(r"\s+")


def normalize_restaurant_name(name: str) -> str:
    normalized = _PAREN_CONTENT_RE.sub("", name).strip()
    normalized = _MULTISPACE_RE.sub(" ", normalized)
    return normalized


def _extract_display_name(payload: dict[str, Any]) -> str:
    display = payload.get("displayName")
    if isinstance(display, dict):
        return display.get("text") or ""
    if isinstance(display, str):
        return display
    return ""


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def parse_place_detail(parser_input: ParserInput) -> ParsedPlaceDetail:
    payload: dict[str, Any] = parser_input.raw_json if isinstance(parser_input.raw_json, dict) else {}

    place_id = payload.get("id") or parser_input.external_id or ""
    canonical_name = _extract_display_name(payload) or parser_input.external_id or ""
    normalized_name = normalize_restaurant_name(canonical_name) if canonical_name else ""

    location = payload.get("location") or {}
    latitude = _to_decimal(location.get("latitude"))
    longitude = _to_decimal(location.get("longitude"))

    average_rating = _to_decimal(payload.get("rating"))
    rating_count = (
        int(payload["userRatingCount"]) if payload.get("userRatingCount") is not None else None
    )

    business_hours = payload.get("regularOpeningHours") or {}
    price_level_raw = payload.get("priceLevel")
    price_level = str(price_level_raw) if price_level_raw is not None else None
    business_status = payload.get("businessStatus")

    restaurant = ParsedRestaurant(
        canonical_name=canonical_name,
        normalized_name=normalized_name,
        address=payload.get("formattedAddress"),
        latitude=latitude,
        longitude=longitude,
        average_rating=average_rating,
        rating_count=rating_count,
        business_hours=business_hours if isinstance(business_hours, dict) else {},
        source_meta=parser_input.source_meta,
        website=payload.get("websiteUri"),
        phone=payload.get("internationalPhoneNumber"),
        price_level=price_level,
        is_closed=(business_status == "CLOSED_PERMANENTLY"),
    )

    external_refs = [
        ParsedExternalRef(
            platform="google_places",
            external_id=place_id,
            external_url=parser_input.source_url
            or (f"https://www.google.com/maps/place/?q=place_id:{place_id}" if place_id else None),
            ref_type="place_detail",
            is_primary=True,
            metadata={"place_id": place_id, "types": payload.get("types") or []},
        )
    ]

    return ParsedPlaceDetail(restaurant=restaurant, external_refs=external_refs, aliases=[])


@dataclass(frozen=True)
class PlaceSearchHit:
    place_id: str
    display_name: str
    formatted_address: str | None
    latitude: Decimal | None
    longitude: Decimal | None
    types: tuple[str, ...] = field(default_factory=tuple)


def parse_search_text(payload: dict[str, Any] | None) -> list[PlaceSearchHit]:
    if not isinstance(payload, dict):
        return []
    places = payload.get("places") or []
    hits: list[PlaceSearchHit] = []
    for place in places:
        if not isinstance(place, dict):
            continue
        location = place.get("location") or {}
        hits.append(
            PlaceSearchHit(
                place_id=place.get("id") or "",
                display_name=_extract_display_name(place),
                formatted_address=place.get("formattedAddress"),
                latitude=_to_decimal(location.get("latitude")),
                longitude=_to_decimal(location.get("longitude")),
                types=tuple(place.get("types") or []),
            )
        )
    return hits
