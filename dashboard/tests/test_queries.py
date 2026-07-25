"""
Tests for dashboard/queries.py using a concrete fake client
(_FakeClickHouseClient), not MagicMock. No real ClickHouse
instance involved.
"""
from queries import get_overdue_trend, get_task_count_by_project_status


class _FakeQueryResult:
    """Minimal stand-in for clickhouse_connect's QueryResult — only
    implements the one attribute these query functions actually read."""

    def __init__(self, result_rows):
        self.result_rows = result_rows


class _FakeClickHouseClient:
    """Records the SQL it was asked to run and returns a canned result."""

    def __init__(self, result_rows=None):
        self._result_rows = result_rows or []
        self.queries: list[str] = []

    def query(self, sql, parameters=None):
        self.queries.append(sql)
        return _FakeQueryResult(self._result_rows)


# ---------------------------------------------------------------------------
# get_overdue_trend
# ---------------------------------------------------------------------------

def test_get_overdue_trend_returns_rows_from_client():
    rows = [("2026-07-20", 3), ("2026-07-21", 5)]
    client = _FakeClickHouseClient(result_rows=rows)

    assert get_overdue_trend(client) == rows


def test_get_overdue_trend_queries_daily_snapshot_table():
    client = _FakeClickHouseClient()
    get_overdue_trend(client)

    assert "daily_task_snapshot" in client.queries[0]
    assert "sum(overdue_count)" in client.queries[0]


def test_get_overdue_trend_empty_table_returns_empty_list():
    client = _FakeClickHouseClient(result_rows=[])
    assert get_overdue_trend(client) == []


# ---------------------------------------------------------------------------
# get_task_count_by_project_status
# ---------------------------------------------------------------------------

def test_get_task_count_by_project_status_returns_rows_from_client():
    rows = [("Website Redesign", "todo", 4), ("(no project)", "done", 2)]
    client = _FakeClickHouseClient(result_rows=rows)

    assert get_task_count_by_project_status(client) == rows


def test_get_task_count_by_project_status_queries_raw_tasks_table():
    client = _FakeClickHouseClient()
    get_task_count_by_project_status(client)

    assert "raw_tasks" in client.queries[0]
    assert "coalesce(project_name" in client.queries[0]


def test_get_task_count_by_project_status_empty_table_returns_empty_list():
    client = _FakeClickHouseClient(result_rows=[])
    assert get_task_count_by_project_status(client) == []
