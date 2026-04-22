from __future__ import annotations

from food_data_ingestion.storage.source_target_repository import SourceTargetRepository


class FakeSession:
    def __init__(self, row):
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
