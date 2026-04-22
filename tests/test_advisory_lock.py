from __future__ import annotations

from food_data_ingestion.db.advisory_lock import PostgresAdvisoryLockManager, build_advisory_lock_key


class FakeSession:
    def __init__(self, rows):
        self.rows = list(rows)
        self.calls = []

    def fetchone(self, query, params):
        self.calls.append((query, params))
        return self.rows.pop(0)



def test_build_advisory_lock_key_is_stable_and_sensitive_to_identifier():
    key_a = build_advisory_lock_key(platform="google_places", resource_type="place_detail", identifier="abc")
    key_b = build_advisory_lock_key(platform="google_places", resource_type="place_detail", identifier="abc")
    key_c = build_advisory_lock_key(platform="google_places", resource_type="place_detail", identifier="xyz")

    assert key_a == key_b
    assert key_a != key_c
    assert isinstance(key_a, int)



def test_postgres_advisory_lock_manager_uses_pg_advisory_lock_functions():
    session = FakeSession(rows=[{"acquired": True}, {"released": True}])
    manager = PostgresAdvisoryLockManager(session)

    acquired = manager.try_acquire(platform="google_places", resource_type="place_detail", identifier="abc")
    released = manager.release(platform="google_places", resource_type="place_detail", identifier="abc")

    assert acquired is True
    assert released is True
    assert "pg_try_advisory_lock" in session.calls[0][0]
    assert "pg_advisory_unlock" in session.calls[1][0]
    assert session.calls[0][1] == session.calls[1][1]
