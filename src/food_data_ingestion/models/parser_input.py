from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ParserInput:
    """parser 實際需要的精簡契約。

    將 parser 與持久化模型 `RawDocumentCreate` 解耦，用意在於：
    - parser 不會被誘惑去讀 crawl_job_id、parse_status 等欄位
    - parser 的測試不需要凑齊只為了持久化才存在的欄位
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
