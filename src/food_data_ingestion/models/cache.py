from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class ApiRequestCacheEntry:
    cache_key: str
    provider: str
    resource_type: str
    cache_version: str = "v1"
    request_fingerprint: str | None = None
    request_params: dict[str, Any] = field(default_factory=dict)
    normalized_url: str | None = None
    status_code: int | None = None
    response_headers: dict[str, Any] | None = None
    response_body: dict[str, Any] | list[Any] | None = None
    response_text: str | None = None
    content_hash: str | None = None
    fetched_at: datetime | None = None
    refresh_after: datetime | None = None
    expires_at: datetime | None = None
    last_accessed_at: datetime | None = None
    hit_count: int = 0
    is_error: bool = False
    error_message: str | None = None
    source_meta: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "ApiRequestCacheEntry":
        return cls(**row)

    def is_fresh(self, *, as_of: datetime) -> bool:
        return self.expires_at is not None and self.expires_at > as_of
