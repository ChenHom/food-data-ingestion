from __future__ import annotations

from food_data_ingestion.storage.source_target_repository import SourceTargetRepository


class FakeSession:
    def __init__(self, row=None):
        self.row = row
        self.calls = []

    def fetchone(self, query, params):
        self.calls.append((query, params))
        return self.row



def test_get_crawl_policy_returns_source_target_policy():
    session = FakeSession(row={"crawl_policy": {"ttl_seconds": 1800, "max_retries": 5}})
    repository = SourceTargetRepository(session)

    policy = repository.get_crawl_policy(42)

    assert policy == {"ttl_seconds": 1800, "max_retries": 5}
    assert session.calls == [
        (
            """
            SELECT crawl_policy
            FROM ingestion.source_targets
            WHERE id = %s
            """,
            (42,),
        )
    ]



def test_get_crawl_policy_returns_empty_dict_when_missing():
    session = FakeSession(row=None)
    repository = SourceTargetRepository(session)

    assert repository.get_crawl_policy(42) == {}



def test_get_by_id_returns_source_target_row():
    session = FakeSession(
        row={
            'id': 42,
            'platform': 'candylife',
            'target_type': 'rss_feed',
            'target_value': 'https://candylife.tw/feed/',
            'region': 'tw',
            'language': 'zh-TW',
            'enabled': True,
            'priority': 10,
            'crawl_policy': {'min_year': 2025, 'limit': 1000},
            'source_meta': {'label': 'candylife-feed'},
        }
    )
    repo = SourceTargetRepository(session)

    target = repo.get_by_id(42)

    assert target['id'] == 42
    assert target['target_value'] == 'https://candylife.tw/feed/'
    assert target['crawl_policy']['min_year'] == 2025
    assert session.calls[0][1] == (42,)
