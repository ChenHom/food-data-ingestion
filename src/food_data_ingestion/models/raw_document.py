from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from hashlib import sha256
import json
from typing import Any

UNSTABLE_HASH_KEYS = {"timestamp", "request_id", "trace_id"}
RAW_PARSE_STATUSES = {"pending", "parsed", "failed", "skipped"}


def _normalize_for_hash(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _normalize_for_hash(nested)
            for key, nested in sorted(value.items())
            if key not in UNSTABLE_HASH_KEYS
        }
    if isinstance(value, list):
        return [_normalize_for_hash(item) for item in value]
    return value


def build_content_hash(
    *,
    raw_json: dict[str, Any] | list[Any] | None = None,
    raw_text: str | None = None,
    raw_html: str | None = None,
) -> str | None:
    if raw_json is not None:
        canonical = json.dumps(
            _normalize_for_hash(raw_json),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return sha256(canonical.encode("utf-8")).hexdigest()

    text_payload = raw_text if raw_text is not None else raw_html
    if text_payload is None:
        return None
    normalized = " ".join(text_payload.split())
    return sha256(normalized.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class RawDocumentCreate:
    platform: str
    document_type: str
    crawl_job_id: int | None = None
    source_target_id: int | None = None
    cache_entry_id: int | None = None
    source_url: str | None = None
    canonical_url: str | None = None
    external_id: str | None = None
    parent_external_id: str | None = None
    http_status: int | None = None
    observed_at: datetime | None = None
    fetched_at: datetime | None = None
    parse_status: str = "pending"
    raw_html: str | None = None
    raw_text: str | None = None
    raw_json: dict[str, Any] | list[Any] | None = None
    response_headers: dict[str, Any] | None = None
    source_meta: dict[str, Any] = field(default_factory=dict)
    parser_version: str | None = None
    parsed_at: datetime | None = None
    content_hash: str | None = None

    def __post_init__(self) -> None:
        if self.raw_html is None and self.raw_text is None and self.raw_json is None:
            raise ValueError("RawDocumentCreate requires at least one raw payload")
        if self.parse_status not in RAW_PARSE_STATUSES:
            raise ValueError(f"invalid raw parse status: {self.parse_status}")
        if self.content_hash is None:
            object.__setattr__(
                self,
                "content_hash",
                build_content_hash(raw_json=self.raw_json, raw_text=self.raw_text, raw_html=self.raw_html),
            )
