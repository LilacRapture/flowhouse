"""
Session-wide safety net: no test in this suite should ever attempt a
real network call to ClickHouse, regardless of Dash's internal layout-
validation behavior.
"""
import pytest

import app
import queries


class _FakeClickHouseClient:
    """Same minimal fake shape as tests/test_queries.py's own —
    duplicated here deliberately: conftest fixtures should stay
    self-contained and not import test-module internals."""

    def query(self, sql, parameters=None):
        class _Result:
            result_rows = []

        return _Result()


@pytest.fixture(autouse=True)
def _no_real_clickhouse_connections(monkeypatch):
    fake_client = _FakeClickHouseClient()
    monkeypatch.setattr(app, "get_client", lambda: fake_client)
    monkeypatch.setattr(queries, "get_client", lambda: fake_client)
