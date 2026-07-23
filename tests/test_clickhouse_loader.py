"""
Tests for src/load/clickhouse_loader.py using a concrete fake client
(_FakeClickHouseClient), not MagicMock — consistent with project test
philosophy. No real ClickHouse instance involved.
"""
from datetime import date

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.load.clickhouse_loader import (
    _CREATE_DAILY_SNAPSHOT_SQL,
    _CREATE_RAW_TASKS_SQL,
    DAILY_SNAPSHOT_TABLE,
    RAW_TASKS_TABLE,
    _normalize_nullable_columns,
    ensure_daily_snapshot_table,
    ensure_raw_tasks_table,
    load_daily_task_snapshot,
    load_raw_tasks,
)


class _FakeClickHouseClient:
    """Records calls instead of talking to a real ClickHouse server."""

    def __init__(self):
        self.commands: list[tuple[str, dict | None]] = []
        self.inserted: list[tuple[str, pd.DataFrame]] = []

    def command(self, cmd: str, parameters: dict | None = None) -> None:
        self.commands.append((cmd, parameters))

    def insert_df(self, table: str, df: pd.DataFrame) -> None:
        self.inserted.append((table, df))


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "snapshot_date": [date(2026, 7, 15)],
            "project_id": [7],
            "project_name": ["Real Project"],
            "owner_id": [1],
            "owner_email": ["a@example.com"],
            "status": ["todo"],
            "task_count": [1],
            "overdue_count": [0],
        }
    )


# ---------------------------------------------------------------------------
# ensure_table
# ---------------------------------------------------------------------------

def test_ensure_table_runs_create_table_sql():
    client = _FakeClickHouseClient()
    ensure_daily_snapshot_table(client)
    assert len(client.commands) == 1
    cmd, params = client.commands[0]
    assert cmd == _CREATE_DAILY_SNAPSHOT_SQL
    assert params is None


def test_load_with_empty_dataframe_only_ensures_table():
    client = _FakeClickHouseClient()
    empty_df = pd.DataFrame(
        columns=[
            "snapshot_date", "project_id", "project_name",
            "owner_id", "owner_email", "status", "task_count", "overdue_count",
        ]
    )
    load_daily_task_snapshot(client, empty_df, "2026-07-15")
    assert len(client.commands) == 1
    assert client.inserted == []


# ---------------------------------------------------------------------------
# load_daily_task_snapshot — non-empty DataFrame
# ---------------------------------------------------------------------------

def test_load_creates_table_then_drops_partition_then_inserts():
    client = _FakeClickHouseClient()
    df = _sample_df()

    load_daily_task_snapshot(client, df, df["snapshot_date"][0])

    assert len(client.commands) == 2
    create_cmd, create_params = client.commands[0]
    drop_cmd, drop_params = client.commands[1]

    assert create_cmd == _CREATE_DAILY_SNAPSHOT_SQL
    assert "DROP PARTITION" in drop_cmd
    assert drop_params == {"snapshot_date": df["snapshot_date"][0]}


def test_load_inserts_the_given_dataframe_into_the_right_table():
    client = _FakeClickHouseClient()
    df = _sample_df()

    load_daily_task_snapshot(client, df, df["snapshot_date"][0])

    assert len(client.inserted) == 1
    table, inserted_df = client.inserted[0]
    assert table == DAILY_SNAPSHOT_TABLE
    assert inserted_df is df


def test_load_does_not_insert_when_dataframe_empty():
    client = _FakeClickHouseClient()
    empty_df = pd.DataFrame(
        columns=[
            "snapshot_date", "project_id", "project_name",
            "owner_id", "owner_email", "status", "task_count", "overdue_count",
        ]
    )
    load_daily_task_snapshot(client, empty_df, "2026-07-15")
    assert client.inserted == []


# ---------------------------------------------------------------------------
# load_raw_tasks
# ---------------------------------------------------------------------------

def _sample_raw_tasks_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [1],
            "title": ["Task A"],
            "description": [""],
            "status": ["todo"],
            "due_date": [date(2026, 7, 1)],
            "owner_id": [1],
            "owner_email": ["a@example.com"],
            "project_id": [None],
            "project_name": [None],
            "created_at": [pd.Timestamp("2026-06-01T10:00:00Z")],
            "updated_at": [pd.Timestamp("2026-06-01T10:00:00Z")],
        }
    )


def test_ensure_raw_tasks_table_runs_create_table_sql():
    client = _FakeClickHouseClient()
    ensure_raw_tasks_table(client)
    assert len(client.commands) == 1
    cmd, params = client.commands[0]
    assert cmd == _CREATE_RAW_TASKS_SQL
    assert params is None


def test_load_raw_tasks_creates_table_then_truncates_then_inserts():
    client = _FakeClickHouseClient()
    df = _sample_raw_tasks_df()

    load_raw_tasks(client, df)

    assert len(client.commands) == 2
    create_cmd, _ = client.commands[0]
    truncate_cmd, _ = client.commands[1]
    assert create_cmd == _CREATE_RAW_TASKS_SQL
    assert "TRUNCATE TABLE" in truncate_cmd
    assert RAW_TASKS_TABLE in truncate_cmd


def test_load_raw_tasks_inserts_into_the_right_table():
    client = _FakeClickHouseClient()
    df = _sample_raw_tasks_df()

    load_raw_tasks(client, df)

    assert len(client.inserted) == 1
    table, inserted_df = client.inserted[0]
    assert table == RAW_TASKS_TABLE
    assert inserted_df["id"].tolist() == df["id"].tolist()


def test_load_raw_tasks_truncates_even_when_dataframe_empty():
    """
    Unlike load_daily_task_snapshot, an empty raw_tasks load still
    truncates — "TaskTracker currently has zero tasks" is a real state
    to reflect, not a reason to leave stale rows.
    """
    client = _FakeClickHouseClient()
    empty_df = pd.DataFrame(columns=list(_sample_raw_tasks_df().columns))

    load_raw_tasks(client, empty_df)

    assert len(client.commands) == 2  # CREATE + TRUNCATE still both ran
    assert client.inserted == []

# ---------------------------------------------------------------------------
# _normalize_nullable_columns — regression for the transform->load parquet
# round-trip (pd.NA reintroduced where build_raw_tasks() had real None)
# ---------------------------------------------------------------------------

def test_normalize_nullable_columns_converts_pd_na_to_none_without_upcasting():
    """
    Regression test: reproduces the exact parquet round-trip a DAG task
    boundary performs (transform writes, load reads back), for both an
    int64[pyarrow]-backed column (project_id) and a date-typed one
    (due_date) — the two behaved differently under bare Series.apply(),
    see the function's docstring.
    """
    df = pd.DataFrame(
        {
            "project_id": pd.Series([7, None], dtype="object"),
            "due_date": pd.Series([date(2026, 7, 1), None], dtype="object"),
        }
    )
    path_str = "/tmp/_normalize_test.parquet"
    pq.write_table(pa.Table.from_pandas(df), path_str)
    roundtripped = pd.read_parquet(path_str, dtype_backend="pyarrow")

    fixed = _normalize_nullable_columns(roundtripped, ["project_id", "due_date"])

    assert fixed["project_id"].tolist() == [7, None]
    assert all(isinstance(v, int) or v is None for v in fixed["project_id"])
    assert fixed["due_date"].tolist() == [date(2026, 7, 1), None]
    assert all(isinstance(v, date) or v is None for v in fixed["due_date"])


def test_load_raw_tasks_normalizes_pd_na_before_insert():
    """
    load_raw_tasks itself must apply the normalization — not just the
    helper in isolation — so a caller handing it a freshly-read-from-
    parquet DataFrame (the real DAG scenario) still inserts clean data.
    """
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "title": ["A", "B"],
            "description": ["", ""],
            "status": ["todo", "done"],
            "due_date": pd.Series([date(2026, 7, 1), None], dtype="object"),
            "owner_id": [1, 1],
            "owner_email": ["a@example.com", "a@example.com"],
            "project_id": pd.Series([7, None], dtype="object"),
            "project_name": pd.Series(["Real Project", None], dtype="object"),
            "created_at": [pd.Timestamp("2026-06-01T10:00:00Z")] * 2,
            "updated_at": [pd.Timestamp("2026-06-01T10:00:00Z")] * 2,
        }
    )
    path_str = "/tmp/_load_raw_tasks_roundtrip_test.parquet"
    pq.write_table(pa.Table.from_pandas(df), path_str)
    roundtripped = pd.read_parquet(path_str, dtype_backend="pyarrow")

    client = _FakeClickHouseClient()
    load_raw_tasks(client, roundtripped)

    _, inserted_df = client.inserted[0]
    assert inserted_df["project_id"].tolist() == [7, None]
    assert inserted_df["due_date"].tolist() == [date(2026, 7, 1), None]
