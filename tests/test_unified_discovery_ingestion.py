from __future__ import annotations

from food_data_ingestion.discovery.models import DiscoveredArticle, DiscoveredPlaceCandidate
from food_data_ingestion.discovery.service import UnifiedDiscoveryIngestionService


class FakeCandidateRepository:
    def __init__(self):
        self.calls = []

    def save_discovered_candidates(self, *, article: DiscoveredArticle, candidates: list[DiscoveredPlaceCandidate]) -> list[int]:
        self.calls.append((article, candidates))
        return [1, 2][: len(candidates)]


def test_unified_discovery_ingestion_service_saves_normalized_candidates():
    article = DiscoveredArticle(
        source_platform='candylife',
        source_url='https://candylife.tw/255labcafe/',
        title='255 LAB café｜台中南屯咖啡廳推薦',
        published_at='2026-04-21T11:45:48+00:00',
        article_type='single_store',
        categories=('台中美食', '台中咖啡'),
        parser_profile='candylife_v1',
        raw_document_id=101,
        extraction_meta={'article_kind': 'single_store'},
    )
    candidates = [
        DiscoveredPlaceCandidate(
            source_platform='candylife',
            source_url='https://candylife.tw/255labcafe/',
            source_name='candylife',
            candidate_name='255 LAB café',
            address='台中市南屯區大墩十一街392號',
            phone='04-22512075',
            opening_hours='平日08:00~16:00；假日10:00~18:00',
            confidence=0.95,
            extraction_method='store_info_block',
            parser_profile='candylife_v1',
            article_type='single_store',
            raw_document_id=101,
        )
    ]
    repo = FakeCandidateRepository()
    service = UnifiedDiscoveryIngestionService(candidate_repository=repo)

    ids = service.ingest_article_candidates(article=article, candidates=candidates)

    assert ids == [1]
    assert repo.calls[0][0].parser_profile == 'candylife_v1'
    assert repo.calls[0][1][0].candidate_name == '255 LAB café'
