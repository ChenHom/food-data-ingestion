from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class SourceTarget:
    id: int
    platform: str
    target_type: str
    target_value: str
    crawl_policy: dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
