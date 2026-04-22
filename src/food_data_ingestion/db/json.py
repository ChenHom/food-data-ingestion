from __future__ import annotations

from typing import Any

from psycopg.types.json import Jsonb


def as_jsonb(value: Any) -> Jsonb:
    return Jsonb(value)
