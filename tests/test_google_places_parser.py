from __future__ import annotations

from decimal import Decimal

from food_data_ingestion.models.parser_input import ParserInput
from food_data_ingestion.parsers.google_places import (
    PlaceSearchHit,
    parse_place_detail,
    parse_search_text,
)


def test_parse_place_detail_extracts_normalized_restaurant_and_external_ref():
    parser_input = ParserInput(
        external_id="ChIJ123",
        source_url="https://maps.google.com/?cid=1",
        raw_json={
            "id": "ChIJ123",
            "displayName": {"text": "鼎泰豐 信義店", "languageCode": "zh-TW"},
            "formattedAddress": "110台北市信義區松高路19號",
            "location": {"latitude": 25.0401, "longitude": 121.5678},
            "rating": 4.4,
            "userRatingCount": 3210,
            "regularOpeningHours": {"weekdayDescriptions": ["星期一: 11:00-21:00"]},
            "websiteUri": "https://www.example.com",
            "internationalPhoneNumber": "+886 2 1234 5678",
            "priceLevel": "PRICE_LEVEL_EXPENSIVE",
            "businessStatus": "OPERATIONAL",
            "types": ["restaurant", "food"],
        },
        source_meta={"request_fingerprint": "abc"},
    )

    parsed = parse_place_detail(parser_input)

    assert parsed.restaurant.canonical_name == "鼎泰豐 信義店"
    assert parsed.restaurant.normalized_name == "鼎泰豐 信義店"
    assert parsed.restaurant.address == "110台北市信義區松高路19號"
    assert parsed.restaurant.latitude == Decimal("25.0401")
    assert parsed.restaurant.longitude == Decimal("121.5678")
    assert parsed.restaurant.average_rating == Decimal("4.4")
    assert parsed.restaurant.rating_count == 3210
    assert parsed.restaurant.business_hours == {"weekdayDescriptions": ["星期一: 11:00-21:00"]}
    assert parsed.restaurant.website == "https://www.example.com"
    assert parsed.restaurant.phone == "+886 2 1234 5678"
    assert parsed.restaurant.price_level == "PRICE_LEVEL_EXPENSIVE"
    assert parsed.restaurant.is_closed is False
    assert parsed.external_refs[0].platform == "google_places"
    assert parsed.external_refs[0].external_id == "ChIJ123"
    assert parsed.external_refs[0].external_url == "https://maps.google.com/?cid=1"
    assert parsed.external_refs[0].is_primary is True
    assert parsed.external_refs[0].metadata["types"] == ["restaurant", "food"]


def test_parse_place_detail_marks_closed_when_business_status_closed_permanently():
    parser_input = ParserInput(
        external_id="ChIJDEAD",
        raw_json={
            "id": "ChIJDEAD",
            "displayName": {"text": "歇業店"},
            "businessStatus": "CLOSED_PERMANENTLY",
        },
    )

    parsed = parse_place_detail(parser_input)

    assert parsed.restaurant.is_closed is True


def test_parse_place_detail_keeps_partial_data_when_fields_missing():
    parser_input = ParserInput(
        external_id="ChIJ999",
        raw_json={"id": "ChIJ999", "displayName": {"text": "無地址小店"}},
    )

    parsed = parse_place_detail(parser_input)

    assert parsed.restaurant.canonical_name == "無地址小店"
    assert parsed.restaurant.address is None
    assert parsed.restaurant.latitude is None
    assert parsed.restaurant.longitude is None
    assert parsed.restaurant.business_hours == {}
    assert parsed.external_refs[0].external_id == "ChIJ999"


def test_parse_search_text_returns_hits_in_order():
    payload = {
        "places": [
            {
                "id": "ChIJ1",
                "displayName": {"text": "鼎泰豐 信義店"},
                "formattedAddress": "110台北市信義區松高路19號",
                "location": {"latitude": 25.04, "longitude": 121.56},
                "types": ["restaurant"],
            },
            {
                "id": "ChIJ2",
                "displayName": {"text": "鼎泰豐 復興店"},
                "formattedAddress": "106台北市大安區復興南路1段218號",
                "location": {"latitude": 25.04, "longitude": 121.54},
            },
        ]
    }

    hits = parse_search_text(payload)

    assert len(hits) == 2
    assert hits[0] == PlaceSearchHit(
        place_id="ChIJ1",
        display_name="鼎泰豐 信義店",
        formatted_address="110台北市信義區松高路19號",
        latitude=Decimal("25.04"),
        longitude=Decimal("121.56"),
        types=("restaurant",),
    )
    assert hits[1].place_id == "ChIJ2"
    assert hits[1].types == ()


def test_parse_search_text_handles_empty_or_invalid_payload():
    assert parse_search_text(None) == []
    assert parse_search_text({}) == []
    assert parse_search_text({"places": []}) == []
