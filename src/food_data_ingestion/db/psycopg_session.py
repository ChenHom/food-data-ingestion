from __future__ import annotations

from typing import Any

import psycopg
from psycopg.rows import dict_row


class PsycopgSession:
    """Wraps a psycopg v3 connection and provides a dict-oriented query interface.

    Responsibilities:
    - Translate SQL results to ``dict[str, Any]``
    - Expose ``fetchone``, ``fetchall``, ``execute``, and ``execute_returning``
    - Expose ``commit`` / ``rollback`` so service / command layer can define transaction boundaries
    """

    def __init__(self, connection: psycopg.Connection) -> None:
        self._conn = connection

    def fetchone(self, query: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
        with self._conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params)
            return cur.fetchone()

    def fetchall(self, query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        with self._conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def execute(self, query: str, params: tuple[Any, ...]) -> None:
        with self._conn.cursor() as cur:
            cur.execute(query, params)

    def execute_returning(self, query: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
        with self._conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params)
            return cur.fetchone()

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()
