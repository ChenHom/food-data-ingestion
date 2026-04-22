from __future__ import annotations

from typing import Any, Protocol


class SessionProtocol(Protocol):
    def fetchone(self, query: str, params: tuple[Any, ...]) -> dict[str, Any] | None: ...


class SourceTargetRepository:
    def __init__(self, session: SessionProtocol):
        self.session = session

    def get_crawl_policy(self, source_target_id: int) -> dict[str, Any]:
        row = self.session.fetchone(
            """
            SELECT crawl_policy
            FROM ingestion.source_targets
            WHERE id = %s
            """,
            (source_target_id,),
        )
        if not row:
            return {}
        crawl_policy = row.get("crawl_policy")
        if isinstance(crawl_policy, dict):
            return crawl_policy
        return {}
