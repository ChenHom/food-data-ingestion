from __future__ import annotations

from decimal import Decimal

from food_data_ingestion.models.restaurant import ParsedExternalRef, ParsedPlaceDetail, ParsedRestaurant
from food_data_ingestion.storage.restaurant_repository import RestaurantRepository


class FakeSession:
    def __init__(self, fetchone_results=None, execute_returning_results=None):
        self.fetchone_results = list(fetchone_results or [])
        self.execute_returning_results = list(execute_returning_results or [])
        self.fetchone_calls = []
        self.execute_calls = []
        self.execute_returning_calls = []

    def fetchone(self, query, params):
        self.fetchone_calls.append((query, params))
        if self.fetchone_results:
            return self.fetchone_results.pop(0)
        return None

    def execute(self, query, params):
        self.execute_calls.append((query, params))

    def execute_returning(self, query, params):
        self.execute_returning_calls.append((query, params))
        if self.execute_returning_results:
            return self.execute_returning_results.pop(0)
        return {"id": 0}


def make_parsed_place_detail() -> ParsedPlaceDetail:
    return ParsedPlaceDetail(
        restaurant=ParsedRestaurant(
            canonical_name="鼎泰豐 信義店",
            normalized_name="鼎泰豐 信義店",
            address="110台北市信義區松高路19號",
            latitude=Decimal("25.0401"),
            longitude=Decimal("121.5678"),
            average_rating=Decimal("4.4"),
            rating_count=3210,
            business_hours={"weekday_text": ["Monday: 11:00-21:00"]},
            website="https://www.example.com",
            phone="02 1234 5678",
            price_level="3",
            source_meta={"source": "google_places"},
        ),
        external_refs=[
            ParsedExternalRef(
                platform="google_places",
                external_id="ChIJ123",
                external_url="https://maps.google.com/?cid=1",
                ref_type="place_detail",
                is_primary=True,
                metadata={"place_id": "ChIJ123"},
            )
        ],
        aliases=["鼎泰豐", "Din Tai Fung"],
    )


def test_upsert_insert_path_creates_restaurant_external_ref_and_aliases():
    session = FakeSession(
        fetchone_results=[None],
        execute_returning_results=[{"id": 101}],
    )
    repository = RestaurantRepository(session)

    restaurant_id = repository.upsert(make_parsed_place_detail())

    assert restaurant_id == 101
    lookup_query, lookup_params = session.fetchone_calls[0]
    assert "FROM ingestion.restaurant_external_refs" in lookup_query
    assert lookup_params == ("google_places", "ChIJ123")

    insert_query, insert_params = session.execute_returning_calls[0]
    assert "INSERT INTO ingestion.restaurants" in insert_query
    assert insert_params[0] == "鼎泰豐 信義店"
    assert insert_params[1] == "鼎泰豐 信義店"
    assert insert_params[6] == "110台北市信義區松高路19號"
    assert insert_params[7] == Decimal("25.0401")
    assert insert_params[8] == Decimal("121.5678")

    assert len(session.execute_calls) == 3
    assert "INSERT INTO ingestion.restaurant_external_refs" in session.execute_calls[0][0]
    assert session.execute_calls[0][1][0] == 101
    assert session.execute_calls[0][1][1] == "google_places"
    assert session.execute_calls[0][1][2] == "ChIJ123"
    assert "INSERT INTO ingestion.restaurant_aliases" in session.execute_calls[1][0]
    assert session.execute_calls[1][1][1] == "鼎泰豐"
    assert session.execute_calls[2][1][1] == "Din Tai Fung"


def test_upsert_update_path_reuses_existing_restaurant_id():
    session = FakeSession(
        fetchone_results=[{"restaurant_id": 55}],
    )
    repository = RestaurantRepository(session)

    restaurant_id = repository.upsert(make_parsed_place_detail())

    assert restaurant_id == 55
    assert session.execute_returning_calls == []
    assert "UPDATE ingestion.restaurants" in session.execute_calls[0][0]
    assert session.execute_calls[0][1][-1] == 55
    assert "INSERT INTO ingestion.restaurant_external_refs" in session.execute_calls[1][0]
    assert session.execute_calls[1][1][0] == 55
