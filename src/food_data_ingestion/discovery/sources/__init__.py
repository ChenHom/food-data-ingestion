from food_data_ingestion.discovery.sources.candylife import (
    CandylifeDiscoveryAdapter,
    build_candylife_adapter,
)
from food_data_ingestion.discovery.sources.supertaste import (
    SupertasteDiscoveryAdapter,
    build_supertaste_adapter,
)

__all__ = [
    "CandylifeDiscoveryAdapter",
    "SupertasteDiscoveryAdapter",
    "build_candylife_adapter",
    "build_supertaste_adapter",
]
