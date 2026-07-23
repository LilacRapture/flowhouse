"""
Integration tests against a REAL ClickHouse instance — not the fake
client used by tests/test_clickhouse_loader.py. Connects using the same
CLICKHOUSE_HOST/PORT/USER/PASSWORD env vars as
src/load/clickhouse_loader.py itself (see .github/workflows/tests.yml
for the CI service container, or run against the local docker-compose
`clickhouse` service).
"""
from datetime import date

import pandas as pd
import pytest

from src.load.clickhouse_loader import (
    DAILY_SNAPSHOT_TABLE,
    RAW_TASKS_TABLE,
    ensure_daily_snapshot_table,
    ensure_raw_tasks_table,
    get_client,
    load_daily_task_snapshot,
    load_raw_tasks,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def client():
    return get_client()


@pytest.fixture(autouse=True)
def _clean_tables(client):
    """
    Real tables persist across test runs (unlike the fake client in
    test_clickhouse_loader.py), so state must be reset explicitly before
    each test.
    """
    ensure_daily_snapshot_table(client)
    ensure_raw_tasks_table(client)
    client.command(f"TRUNCATE TABLE {DAILY_SNAPSHOT_TABLE}")
    client.command(f"TRUNCATE TABLE {RAW_TASKS_TABLE}")


def _snapshot_df(snapshot_date=date(2026, 7, 15), project_id=0, status="todo"):
    return pd.DataFrame({
        "snapshot_date": [snapshot_date],
        "project_id": [project_id],
        "project_name": ["(no project)" if project_id == 0 else "Real Project"],
        "owner_id": [1],
        "owner_email": ["a@example.com"],
        "status": [status],
        "task_count": [1],
        "overdue_count": [0],
    })


def _raw_tasks_df():
    return pd.DataFrame({
        "id": [1, 2],
        "title": ["A", "B"],
        "description": ["", ""],
        "status": ["todo", "done"],
        "due_date": [date(2026, 7, 1), None],
        "owner_id": [1, 1],
        "owner_email": ["a@example.com", "a@example.com"],
        "project_id": [7, None],
        "project_name": ["Real Project", None],
        "created_at": [
            pd.Timestamp("2026-06-01T10:00:00Z"),
            pd.Timestamp("2026-06-02T10:00:00Z"),
        ],
        "updated_at": [
            pd.Timestamp("2026-06-01T10:00:00Z"),
            pd.Timestamp("2026-06-02T10:00:00Z"),
        ],
    })


# ---------------------------------------------------------------------------
# Table creation
# ---------------------------------------------------------------------------

def test_ensure_daily_snapshot_table_creates_valid_table(client):
    result = client.query(f"EXISTS TABLE {DAILY_SNAPSHOT_TABLE}")
    assert result.result_rows[0][0] == 1


def test_ensure_raw_tasks_table_creates_valid_table(client):
    result = client.query(f"EXISTS TABLE {RAW_TASKS_TABLE}")
    assert result.result_rows[0][0] == 1


# ---------------------------------------------------------------------------
# daily_task_snapshot — per-day partition refresh (ADR-011)
# ---------------------------------------------------------------------------

def test_load_daily_task_snapshot_inserts_rows(client):
    load_daily_task_snapshot(client, _snapshot_df(), date(2026, 7, 15))

    result = client.query(f"SELECT count() FROM {DAILY_SNAPSHOT_TABLE}")
    assert result.result_rows[0][0] == 1


def test_reloading_same_day_replaces_not_duplicates(client):
    """The core claim of ADR-011: a second run for the SAME
    snapshot_date must not accumulate duplicate rows."""
    snapshot_date = date(2026, 7, 15)
    load_daily_task_snapshot(client, _snapshot_df(snapshot_date, status="todo"), snapshot_date)
    load_daily_task_snapshot(client, _snapshot_df(snapshot_date, status="done"), snapshot_date)

    result = client.query(
        f"SELECT status, count() FROM {DAILY_SNAPSHOT_TABLE} "
        f"WHERE snapshot_date = %(d)s GROUP BY status",
        parameters={"d": snapshot_date},
    )
    assert result.result_rows == [("done", 1)]


def test_reloading_one_day_does_not_touch_other_days(client):
    day1, day2 = date(2026, 7, 14), date(2026, 7, 15)
    load_daily_task_snapshot(client, _snapshot_df(day1), day1)
    load_daily_task_snapshot(client, _snapshot_df(day2), day2)
    load_daily_task_snapshot(client, _snapshot_df(day2, status="done"), day2)

    result = client.query(f"SELECT count() FROM {DAILY_SNAPSHOT_TABLE}")
    assert result.result_rows[0][0] == 2


# ---------------------------------------------------------------------------
# raw_tasks — whole-table replace
# ---------------------------------------------------------------------------

def test_load_raw_tasks_inserts_rows_with_correct_nulls(client):
    load_raw_tasks(client, _raw_tasks_df())

    result = client.query(
        f"SELECT id, due_date, project_id, project_name FROM {RAW_TASKS_TABLE} ORDER BY id"
    )
    rows = result.result_rows
    assert rows[0] == (1, date(2026, 7, 1), 7, "Real Project")
    assert rows[1] == (2, None, None, None)


def test_reloading_raw_tasks_replaces_entire_table(client):
    load_raw_tasks(client, _raw_tasks_df())
    load_raw_tasks(client, _raw_tasks_df().iloc[[0]])

    result = client.query(f"SELECT count() FROM {RAW_TASKS_TABLE}")
    assert result.result_rows[0][0] == 1
