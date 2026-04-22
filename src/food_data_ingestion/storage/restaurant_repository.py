from __future__ import annotations

from typing import Any, Protocol

from food_data_ingestion.models.restaurant import ParsedPlaceDetail


def _normalize_alias(alias: str) -> str:
    return " ".join(alias.strip().split()).lower()


class SessionProtocol(Protocol):
    def fetchone(self, query: str, params: tuple[Any, ...]) -> dict[str, Any] | None: ...

    def execute(self, query: str, params: tuple[Any, ...]) -> None: ...

    def execute_returning(self, query: str, params: tuple[Any, ...]) -> dict[str, Any] | None: ...


class RestaurantRepository:
    def __init__(self, session: SessionProtocol):
        self.session = session

    def upsert(self, parsed: ParsedPlaceDetail) -> int:
        primary_ref = next((ref for ref in parsed.external_refs if ref.is_primary), parsed.external_refs[0])
        existing = self.session.fetchone(
            """
            SELECT restaurant_id
            FROM ingestion.restaurant_external_refs
            WHERE platform = %s AND external_id = %s
            """,
            (primary_ref.platform, primary_ref.external_id),
        )

        if existing:
            restaurant_id = int(existing["restaurant_id"])
            self.session.execute(
                """
                UPDATE ingestion.restaurants
                SET canonical_name = %s,
                    normalized_name = %s,
                    branch_name = %s,
                    country = %s,
                    city = %s,
                    district = %s,
                    address = %s,
                    latitude = %s,
                    longitude = %s,
                    phone = %s,
                    website = %s,
                    price_level = %s,
                    average_rating = %s,
                    rating_count = %s,
                    business_hours = %s,
                    is_closed = %s,
                    source_meta = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (
                    parsed.restaurant.canonical_name,
                    parsed.restaurant.normalized_name,
                    parsed.restaurant.branch_name,
                    parsed.restaurant.country,
                    parsed.restaurant.city,
                    parsed.restaurant.district,
                    parsed.restaurant.address,
                    parsed.restaurant.latitude,
                    parsed.restaurant.longitude,
                    parsed.restaurant.phone,
                    parsed.restaurant.website,
                    parsed.restaurant.price_level,
                    parsed.restaurant.average_rating,
                    parsed.restaurant.rating_count,
                    parsed.restaurant.business_hours,
                    parsed.restaurant.is_closed,
                    parsed.restaurant.source_meta,
                    restaurant_id,
                ),
            )
        else:
            row = self.session.execute_returning(
                """
                INSERT INTO ingestion.restaurants (
                    canonical_name,
                    normalized_name,
                    branch_name,
                    country,
                    city,
                    district,
                    address,
                    latitude,
                    longitude,
                    phone,
                    website,
                    price_level,
                    average_rating,
                    rating_count,
                    business_hours,
                    is_closed,
                    source_meta
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                RETURNING id
                """,
                (
                    parsed.restaurant.canonical_name,
                    parsed.restaurant.normalized_name,
                    parsed.restaurant.branch_name,
                    parsed.restaurant.country,
                    parsed.restaurant.city,
                    parsed.restaurant.district,
                    parsed.restaurant.address,
                    parsed.restaurant.latitude,
                    parsed.restaurant.longitude,
                    parsed.restaurant.phone,
                    parsed.restaurant.website,
                    parsed.restaurant.price_level,
                    parsed.restaurant.average_rating,
                    parsed.restaurant.rating_count,
                    parsed.restaurant.business_hours,
                    parsed.restaurant.is_closed,
                    parsed.restaurant.source_meta,
                ),
            )
            if not row or "id" not in row:
                raise RuntimeError("failed to create restaurant")
            restaurant_id = int(row["id"])

        for ref in parsed.external_refs:
            self.session.execute(
                """
                INSERT INTO ingestion.restaurant_external_refs (
                    restaurant_id,
                    platform,
                    external_id,
                    external_url,
                    ref_type,
                    is_primary,
                    metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (platform, external_id) DO UPDATE
                SET restaurant_id = EXCLUDED.restaurant_id,
                    external_url = EXCLUDED.external_url,
                    ref_type = EXCLUDED.ref_type,
                    is_primary = EXCLUDED.is_primary,
                    metadata = EXCLUDED.metadata
                """,
                (
                    restaurant_id,
                    ref.platform,
                    ref.external_id,
                    ref.external_url,
                    ref.ref_type,
                    ref.is_primary,
                    ref.metadata,
                ),
            )

        for alias in parsed.aliases:
            self.session.execute(
                """
                INSERT INTO ingestion.restaurant_aliases (
                    restaurant_id,
                    alias_name,
                    normalized_alias
                ) VALUES (%s, %s, %s)
                ON CONFLICT (restaurant_id, normalized_alias) DO NOTHING
                """,
                (restaurant_id, alias, _normalize_alias(alias)),
            )

        return restaurant_id
