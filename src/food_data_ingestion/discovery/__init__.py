from .adapter import BuildContext, DiscoveryAdapterProtocol, DiscoveryDeps
from .models import DiscoveredArticle, DiscoveredPlaceCandidate
from .service import UnifiedDiscoveryIngestionService

# 說明：`registry` 與 `sources.*` 刻意不在這裡 re-export，
# 因為它們會 import `storage`，而 `storage` 本身又會從 `discovery.models` import。
# 在這個最上層 import 會造成循環依賴。呼叫端請這樣使用：
#   from food_data_ingestion.discovery.registry import DEFAULT_FACTORY

__all__ = [
    'BuildContext',
    'DiscoveredArticle',
    'DiscoveredPlaceCandidate',
    'DiscoveryAdapterProtocol',
    'DiscoveryDeps',
    'UnifiedDiscoveryIngestionService',
]
