from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol, TypedDict


class FetchResult(TypedDict):
    provider: str
    resource_type: str
    cache_key: str
    normalized_url: str | None
    request_params: dict[str, Any]
    status_code: int | None
    response_headers: dict[str, Any] | None
    response_body: dict[str, Any] | list[Any] | None
    response_text: str | None
    fetched_at: datetime
    expires_at: datetime
    refresh_after: datetime | None
    is_error: bool
    error_message: str | None
    source_meta: dict[str, Any]


class CacheRepositoryProtocol(Protocol):
    def get_valid(self, cache_key: str, *, as_of: datetime): ...


class ConnectorProtocol(Protocol):
    def fetch_place_detail(self, place_id: str, *, fields: list[str] | None = None, language: str = "zh-TW") -> FetchResult: ...
