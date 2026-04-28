from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ParserInput:
    """Narrow contract a parser actually consumes.

    Decouples parser from persistence model `RawDocumentCreate` so that:
    - Parsers are not tempted to read crawl_job_id, parse_status, etc.
    - Parser tests don't have to construct persistence-only fields.
    """

    raw_json: dict[str, Any] | list[Any] | None = None
    raw_html: str | None = None
    raw_text: str | None = None
    external_id: str | None = None
    source_url: str | None = None
    source_meta: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_fetch_result(cls, fetch_result: dict[str, Any], *, external_id: str | None = None) -> "ParserInput":
        body = fetch_result.get("response_body")
        return cls(
            raw_json=body if isinstance(body, (dict, list)) else None,
            raw_text=fetch_result.get("response_text"),
            raw_html=fetch_result.get("response_html"),
            external_id=external_id,
            source_url=fetch_result.get("normalized_url"),
            source_meta=fetch_result.get("source_meta") or {},
        )
