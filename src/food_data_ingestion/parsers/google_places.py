from __future__ import annotations

from decimal import Decimal
import re

from food_data_ingestion.models.raw_document import RawDocumentCreate
from food_data_ingestion.models.restaurant import ParsedExternalRef, ParsedPlaceDetail, ParsedRestaurant

_PAREN_CONTENT_RE = re.compile(r"\s*\([^)]*\)")
_MULTISPACE_RE = re.compile(r"\s+")


def normalize_restaurant_name(name: str) -> str:
    normalized = _PAREN_CONTENT_RE.sub("", name).strip()
    normalized = _MULTISPACE_RE.sub(" ", normalized)
    return normalized


def parse_place_detail(raw_document: RawDocumentCreate) -> ParsedPlaceDetail:
    result = {}
    if isinstance(raw_document.raw_json, dict):
        result = raw_document.raw_json.get("result") or {}

    place_id = result.get("place_id") or raw_document.external_id or ""
    canonical_name = result.get("name") or raw_document.external_id or ""
    normalized_name = normalize_restaurant_name(canonical_name) if canonical_name else ""

    location = ((result.get("geometry") or {}).get("location") or {}) if isinstance(result, dict) else {}
    latitude = Decimal(str(location["lat"])) if "lat" in location and location["lat"] is not None else None
    longitude = Decimal(str(location["lng"])) if "lng" in location and location["lng"] is not None else None
    average_rating = Decimal(str(result["rating"])) if result.get("rating") is not None else None
    rating_count = int(result["user_ratings_total"]) if result.get("user_ratings_total") is not None else None

    restaurant = ParsedRestaurant(
        canonical_name=canonical_name,
        normalized_name=normalized_name,
        address=result.get("formatted_address"),
        latitude=latitude,
        longitude=longitude,
        average_rating=average_rating,
        rating_count=rating_count,
        business_hours=result.get("opening_hours") or {},
        source_meta=raw_document.source_meta,
        website=result.get("website"),
        phone=result.get("formatted_phone_number") or result.get("international_phone_number"),
        price_level=str(result.get("price_level")) if result.get("price_level") is not None else None,
        is_closed=(result.get("business_status") == "CLOSED_PERMANENTLY"),
    )

    external_refs = [
        ParsedExternalRef(
            platform="google_places",
            external_id=place_id,
            external_url=result.get("url") or raw_document.source_url,
            ref_type="place_detail",
            is_primary=True,
            metadata={"place_id": place_id},
        )
    ]

    return ParsedPlaceDetail(restaurant=restaurant, external_refs=external_refs, aliases=[])
