from __future__ import annotations

from dataclasses import astuple
from datetime import datetime
import re
from typing import Any, Protocol

from food_data_ingestion.models.cache import ApiRequestCacheEntry


class SessionProtocol(Protocol):
    def fetchone(self, query: str, params: tuple[Any, ...]) -> dict[str, Any] | None: ...
    def execute(self, query: str, params: tuple[Any, ...]) -> None: ...


def _normalize_key_part(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9_]+", "-", value.strip().lower())
    return normalized.strip("-")


def build_cache_key(provider: str, resource_type: str, identifier: str, *, version: str = "v1") -> str:
    return ":".join(
        [
            _normalize_key_part(provider),
            _normalize_key_part(version),
            _normalize_key_part(resource_type),
            _normalize_key_part(identifier),
        ]
    )


class ApiRequestCacheRepository:
    def __init__(self, session: SessionProtocol):
        self.session = session

    def get_valid(self, cache_key: str, *, as_of: datetime) -> ApiRequestCacheEntry | None:
        row = self.session.fetchone(
            """
            SELECT
                cache_key,
                provider,
                resource_type,
                cache_version,
                request_fingerprint,
                request_params,
                normalized_url,
                status_code,
                response_headers,
                response_body,
                response_text,
                content_hash,
                fetched_at,
                refresh_after,
                expires_at,
                last_accessed_at,
                hit_count,
                is_error,
                error_message,
                source_meta
            FROM ingestion.api_request_cache
            WHERE cache_key = %s
            """,
            (cache_key,),
        )
        if not row:
            return None

        entry = ApiRequestCacheEntry.from_row(row)
        if not entry.is_fresh(as_of=as_of):
            return None
        return entry

    def mark_hit(self, cache_key: str, *, accessed_at: datetime) -> None:
        self.session.execute(
            """
            UPDATE ingestion.api_request_cache
            SET last_accessed_at = %s,
                hit_count = hit_count + 1,
                updated_at = NOW()
            WHERE cache_key = %s
            """,
            (accessed_at, cache_key),
        )

    def upsert(self, entry: ApiRequestCacheEntry) -> None:
        self.session.execute(
            """
            INSERT INTO ingestion.api_request_cache (
                cache_key,
                provider,
                resource_type,
                cache_version,
                request_fingerprint,
                request_params,
                normalized_url,
                status_code,
                response_headers,
                response_body,
                content_hash,
                response_text,
                fetched_at,
                refresh_after,
                expires_at,
                last_accessed_at,
                hit_count,
                is_error,
                error_message,
                source_meta
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
            ON CONFLICT (cache_key) DO UPDATE
            SET provider = EXCLUDED.provider,
                resource_type = EXCLUDED.resource_type,
                cache_version = EXCLUDED.cache_version,
                request_fingerprint = EXCLUDED.request_fingerprint,
                request_params = EXCLUDED.request_params,
                normalized_url = EXCLUDED.normalized_url,
                status_code = EXCLUDED.status_code,
                response_headers = EXCLUDED.response_headers,
                response_body = EXCLUDED.response_body,
                response_text = EXCLUDED.response_text,
                content_hash = EXCLUDED.content_hash,
                fetched_at = EXCLUDED.fetched_at,
                refresh_after = EXCLUDED.refresh_after,
                expires_at = EXCLUDED.expires_at,
                last_accessed_at = EXCLUDED.last_accessed_at,
                is_error = EXCLUDED.is_error,
                error_message = EXCLUDED.error_message,
                source_meta = EXCLUDED.source_meta,
                updated_at = NOW()
            """,
            (
                entry.cache_key,
                entry.provider,
                entry.resource_type,
                entry.cache_version,
                entry.request_fingerprint,
                entry.request_params,
                entry.normalized_url,
                entry.status_code,
                entry.response_headers,
                entry.response_body,
                entry.content_hash,
                entry.response_text,
                entry.fetched_at,
                entry.refresh_after,
                entry.expires_at,
                entry.last_accessed_at,
                entry.hit_count,
                entry.is_error,
                entry.error_message,
                entry.source_meta,
            ),
        )
