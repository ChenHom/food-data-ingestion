from __future__ import annotations

from hashlib import sha256
from typing import Protocol


class SessionProtocol(Protocol):
    def fetchone(self, query: str, params: tuple[object, ...]) -> dict[str, object] | None: ...


def build_advisory_lock_key(*, platform: str, resource_type: str, identifier: str) -> int:
    canonical = f"{platform}:{resource_type}:{identifier}".encode("utf-8")
    digest = sha256(canonical).digest()[:8]
    return int.from_bytes(digest, byteorder="big", signed=True)


class PostgresAdvisoryLockManager:
    def __init__(self, session: SessionProtocol):
        self.session = session

    def try_acquire(self, *, platform: str, resource_type: str, identifier: str) -> bool:
        key = build_advisory_lock_key(platform=platform, resource_type=resource_type, identifier=identifier)
        row = self.session.fetchone("SELECT pg_try_advisory_lock(%s) AS acquired", (key,))
        return bool(row and row.get("acquired"))

    def release(self, *, platform: str, resource_type: str, identifier: str) -> bool:
        key = build_advisory_lock_key(platform=platform, resource_type=resource_type, identifier=identifier)
        row = self.session.fetchone("SELECT pg_advisory_unlock(%s) AS released", (key,))
        return bool(row and row.get("released"))
