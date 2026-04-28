from __future__ import annotations

from typing import Any

import psycopg
from psycopg.rows import dict_row


class PsycopgSession:
    """包裝 psycopg v3 connection，提供以 dict 為導向的查詢介面。

    職責：
    - 將 SQL 結果轉換為 ``dict[str, Any]``
    - 對外提供 ``fetchone``、``fetchall``、``execute``、``execute_returning``
    - 對外提供 ``commit`` / ``rollback``，讓 service / command 層能定義 transaction 邊界
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
