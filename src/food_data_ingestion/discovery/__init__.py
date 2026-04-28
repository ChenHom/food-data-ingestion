from .adapter import BuildContext, DiscoveryAdapterProtocol, DiscoveryDeps
from .models import DiscoveredArticle, DiscoveredPlaceCandidate
from .service import UnifiedDiscoveryIngestionService

# Note: `registry` and `sources.*` are intentionally NOT re-exported here
# because they pull in `storage`, which itself imports from `discovery.models`.
# Importing them at this top level would create a cycle. Callers should do:
#   from food_data_ingestion.discovery.registry import DEFAULT_FACTORY

__all__ = [
    'BuildContext',
    'DiscoveredArticle',
    'DiscoveredPlaceCandidate',
    'DiscoveryAdapterProtocol',
    'DiscoveryDeps',
    'UnifiedDiscoveryIngestionService',
]
