"""Adapter factory 與預設 registry。

新增一個公開 source 的步驟：
  1. 在 `discovery/sources/<name>.py` 下實作一個 `DiscoveryAdapterProtocol`
  2. 提供 `build_<name>_adapter(ctx)` builder
  3. 在 `DEFAULT_FACTORY` 裡註冊
不需要另外寫一個 `jobs/run_<name>.py` script。
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
            raise KeyError(f"尚未為 platform={platform!r} 註冊任何 discovery adapter") from exc
        return builder(ctx)

    def register(self, platform: str, builder: AdapterBuilder) -> None:
        self.builders[platform] = builder


DEFAULT_FACTORY = DiscoveryAdapterFactory(
    builders={
        "candylife": build_candylife_adapter,
        "supertaste": build_supertaste_adapter,
    }
)
