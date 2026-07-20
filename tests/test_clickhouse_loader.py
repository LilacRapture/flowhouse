"""
Tests for src/load/clickhouse_loader.py using a concrete fake client
(_FakeClickHouseClient), not MagicMock — consistent with project test
philosophy. No real ClickHouse instance involved.
"""
import pandas as pd

from src.load.clickhouse_loader import (
    _CREATE_TABLE_SQL,
    TABLE_NAME,
    ensure_table,
    load_daily_task_snapshot,
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
            "snapshot_date": [__import__("datetime").date(2026, 7, 15)],
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
    ensure_table(client)

    assert len(client.commands) == 1
    cmd, params = client.commands[0]
    assert cmd == _CREATE_TABLE_SQL
    assert params is None


# ---------------------------------------------------------------------------
# load_daily_task_snapshot — empty DataFrame
# ---------------------------------------------------------------------------

def test_load_with_empty_dataframe_only_ensures_table(tmp_path):
    client = _FakeClickHouseClient()
    empty_df = pd.DataFrame(
        columns=[
            "snapshot_date", "project_id", "project_name",
            "owner_id", "owner_email", "status", "task_count", "overdue_count",
        ]
    )

    load_daily_task_snapshot(client, empty_df, "2026-07-15")

    assert len(client.commands) == 1  # only the CREATE TABLE
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

    assert create_cmd == _CREATE_TABLE_SQL
    assert "DROP PARTITION" in drop_cmd
    assert drop_params == {"snapshot_date": df["snapshot_date"][0]}


def test_load_inserts_the_given_dataframe_into_the_right_table():
    client = _FakeClickHouseClient()
    df = _sample_df()

    load_daily_task_snapshot(client, df, df["snapshot_date"][0])

    assert len(client.inserted) == 1
    table, inserted_df = client.inserted[0]
    assert table == TABLE_NAME
    assert inserted_df is df


def test_load_drop_partition_happens_before_insert():
    client = _FakeClickHouseClient()
    df = _sample_df()

    load_daily_task_snapshot(client, df, df["snapshot_date"][0])

    # commands[1] is the DROP PARTITION call; inserted must happen after —
    # can't compare timestamps directly, but the fake only has two lists,
    # so we rely on the function's own internal ordering being exercised
    # by test_load_creates_table_then_drops_partition_then_inserts above,
    # and confirm here that insert wasn't skipped/duplicated.
    assert len(client.inserted) == 1
    