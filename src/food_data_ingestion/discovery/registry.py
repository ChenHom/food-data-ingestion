"""Adapter factory + default registry.

Adding a new public source means:
  1. Implement a `DiscoveryAdapterProtocol` under `discovery/sources/<name>.py`
  2. Provide a `build_<name>_adapter(ctx)` builder
  3. Register it in `DEFAULT_FACTORY`
No new `jobs/run_<name>.py` script is required.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from food_data_ingestion.discovery.adapter import (
    BuildContext,
    DiscoveryAdapterProtocol,
)
from food_data_ingestion.discovery.sources import (
    build_candylife_adapter,
    build_supertaste_adapter,
)


AdapterBuilder = Callable[[BuildContext], DiscoveryAdapterProtocol]


@dataclass
class DiscoveryAdapterFactory:
    builders: dict[str, AdapterBuilder] = field(default_factory=dict)

    def platforms(self) -> list[str]:
        return list(self.builders.keys())

    def build(self, platform: str, ctx: BuildContext) -> DiscoveryAdapterProtocol:
        try:
            builder = self.builders[platform]
        except KeyError as exc:
            raise KeyError(f"no discovery adapter registered for platform={platform!r}") from exc
        return builder(ctx)

    def register(self, platform: str, builder: AdapterBuilder) -> None:
        self.builders[platform] = builder


DEFAULT_FACTORY = DiscoveryAdapterFactory(
    builders={
        "candylife": build_candylife_adapter,
        "supertaste": build_supertaste_adapter,
    }
)
