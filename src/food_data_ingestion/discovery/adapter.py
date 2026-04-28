"""Discovery adapter 抽象層。

每一個公開來源的 discovery 流程（candylife / supertaste / ...）都會以
`DiscoveryAdapterProtocol` 的形式對外提供。Runner 只需要透過 factory 建立
adapter 並呼叫 `run(source_target=..., deps=...)`，因此新增一個來源不需要
另外寫一個 `jobs/run_<x>.py` script。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass
class DiscoveryDeps:
    """單次 adapter 執行所需的 repository 與 transaction manager 集合。

    所有欄位預設為 None，呼叫端可以只傳入部分欄位；當某個欄位是 None 時，
    adapter 會自動回退到自己內建的 in-memory 替身。
    """

    raw_repository: Any | None = None
    candidate_repository: Any | None = None
    crawl_job_repository: Any | None = None
    cache_repository: Any | None = None
    transaction_manager: Any | None = None


@dataclass
class BuildContext:
    """傳遞給 adapter builder 的場景資訊。

    這裡只放會影響「要建立哪一種 fetcher」的旗標；個別 target 的調整參數
    （min_year、limit、...）則由 `source_target.crawl_policy` 提供。
    """

    use_stub_fetcher: bool = False
    extra: dict[str, Any] = field(default_factory=dict)


class DiscoveryAdapterProtocol(Protocol):
    """針對單一 source_target（或使用預設設定）執行一次 discovery。"""

    platform: str

    def run(
        self,
        *,
        source_target: dict[str, Any] | None,
        deps: DiscoveryDeps,
    ) -> dict[str, Any]: ...
