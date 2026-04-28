"""Discovery adapter abstraction.

Each public-source discovery flow (candylife / supertaste / ...) is exposed
as a `DiscoveryAdapterProtocol` implementation. The runner only knows how to
build adapters via a factory and call `run(source_target=..., deps=...)`,
so adding a new source no longer requires a new `jobs/run_<x>.py` script.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass
class DiscoveryDeps:
    """Repositories + transaction manager wired into a single adapter run.

    All fields default to None so callers can pass a partial set; adapters
    fall back to their own in-memory stand-ins when a field is None.
    """

    raw_repository: Any | None = None
    candidate_repository: Any | None = None
    crawl_job_repository: Any | None = None
    cache_repository: Any | None = None
    transaction_manager: Any | None = None


@dataclass
class BuildContext:
    """Context passed to adapter builders.

    Only flags that influence which fetcher to construct live here; per-target
    knobs (min_year, limit, ...) come from `source_target.crawl_policy`.
    """

    use_stub_fetcher: bool = False
    extra: dict[str, Any] = field(default_factory=dict)


class DiscoveryAdapterProtocol(Protocol):
    """Run discovery for a single source_target (or a default-configured run)."""

    platform: str

    def run(
        self,
        *,
        source_target: dict[str, Any] | None,
        deps: DiscoveryDeps,
    ) -> dict[str, Any]: ...
