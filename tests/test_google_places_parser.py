from __future__ import annotations

from decimal import Decimal

from food_data_ingestion.models.raw_document import RawDocumentCreate
from food_data_ingestion.parsers.google_places import parse_place_detail


def test_parse_place_detail_extracts_normalized_restaurant_and_external_ref():
    raw_document = RawDocumentCreate(
        platform="google_places",
        document_type="place_detail",
        external_id="ChIJ123",
        source_url="https://maps.google.com/?cid=1",
        raw_json={
            "result": {
                "place_id": "ChIJ123",
                "name": "鼎泰豐 信義店",
                "formatted_address": "110台北市信義區松高路19號",
                "geometry": {"location": {"lat": 25.0401, "lng": 121.5678}},
                "rating": 4.4,
                "user_ratings_total": 3210,
                "opening_hours": {"weekday_text": ["Monday: 11:00-21:00"]},
                "website": "https://www.example.com",
                "url": "https://maps.google.com/?cid=1",
                "formatted_phone_number": "02 1234 5678",
                "price_level": 3,
                "business_status": "OPERATIONAL",
            }
        },
        source_meta={"request_fingerprint": "abc"},
    )

    parsed = parse_place_detail(raw_document)

    assert parsed.restaurant.canonical_name == "鼎泰豐 信義店"
    assert parsed.restaurant.normalized_name == "鼎泰豐 信義店"
    assert parsed.restaurant.address == "110台北市信義區松高路19號"
    assert parsed.restaurant.latitude == Decimal("25.0401")
    assert parsed.restaurant.longitude == Decimal("121.5678")
    assert parsed.restaurant.average_rating == Decimal("4.4")
    assert parsed.restaurant.rating_count == 3210
    assert parsed.restaurant.business_hours == {"weekday_text": ["Monday: 11:00-21:00"]}
    assert parsed.restaurant.website == "https://www.example.com"
    assert parsed.restaurant.phone == "02 1234 5678"
    assert parsed.restaurant.price_level == "3"
    assert parsed.restaurant.is_closed is False
    assert parsed.external_refs[0].platform == "google_places"
    assert parsed.external_refs[0].external_id == "ChIJ123"
    assert parsed.external_refs[0].external_url == "https://maps.google.com/?cid=1"
    assert parsed.external_refs[0].is_primary is True


def test_parse_place_detail_keeps_partial_data_when_fields_missing():
    raw_document = RawDocumentCreate(
        platform="google_places",
        document_type="place_detail",
        external_id="ChIJ999",
        raw_json={"result": {"place_id": "ChIJ999", "name": "無地址小店"}},
    )

    parsed = parse_place_detail(raw_document)

    assert parsed.restaurant.canonical_name == "無地址小店"
    assert parsed.restaurant.address is None
    assert parsed.restaurant.latitude is None
    assert parsed.restaurant.longitude is None
    assert parsed.restaurant.business_hours == {}
    assert parsed.external_refs[0].external_id == "ChIJ999"
