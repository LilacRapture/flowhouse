"""
Tests for src/transform/spark_ops.py — the PySpark transform engine.

Mirrors tests/test_pandas_ops.py's coverage 
(grouping, overdue definition, sentinel vs. literal-null
project handling). Also includes dedicated dtype checks for
snapshot_date/due_date/created_at after .toPandas().
"""
import logging
from datetime import date, datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.transform.spark_ops import build_daily_task_snapshot, build_raw_tasks, get_spark

logger = logging.getLogger(__name__)

SNAPSHOT_DATE = "2026-07-15"
SNAPSHOT_DATE_OBJ = date(2026, 7, 15)
_TASKS_ARROW_SCHEMA = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("title", pa.string()),
    pa.field("description", pa.string()),
    pa.field("status", pa.string()),
    pa.field("due_date", pa.string()),
    pa.field("owner", pa.struct([
        pa.field("id", pa.int64()),
        pa.field("email", pa.string()),
        pa.field("full_name", pa.string()),
    ])),
    pa.field("project", pa.struct([
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
    ])),
    pa.field("created_at", pa.string()),
    pa.field("updated_at", pa.string()),
])



@pytest.fixture(scope="session")
def spark():
    """
    One SparkSession for the whole test session — starting a fresh JVM
    per test would be prohibitively slow (same reasoning as local[2] in
    get_spark() — bounded resource use, here bounded test time instead).
    """
    session = get_spark()
    yield session
    session.stop()


def _write_tasks_parquet(tmp_path, records: list[dict]) -> str:
    """
    Explicit schema (not left to pyarrow's per-batch type inference) —
    without it, a batch where EVERY record has project=None (or
    due_date=None) gets inferred as pyarrow's null type instead of a
    struct/string, which Spark then can't extract struct fields from.
    """
    path = str(tmp_path / "tasks_test.parquet")
    table = pa.Table.from_pylist(records, schema=_TASKS_ARROW_SCHEMA)
    pq.write_table(table, path)
    return path


def _task(
    task_id: int,
    status: str = "todo",
    due_date=None,
    owner_id: int = 1,
    owner_email: str = "owner@example.com",
    project=None,
) -> dict:
    return {
        "id": task_id,
        "title": f"Task {task_id}",
        "description": "",
        "status": status,
        "due_date": due_date,
        "owner": {"id": owner_id, "email": owner_email, "full_name": "Owner Name"},
        "project": project,
        "created_at": "2026-06-01T10:00:00Z",
        "updated_at": "2026-06-01T10:00:00Z",
    }


# ---------------------------------------------------------------------------
# build_daily_task_snapshot — grouping (mirrors test_pandas_ops.py)
# ---------------------------------------------------------------------------

def test_single_task_produces_single_row(spark, tmp_path):
    path = _write_tasks_parquet(tmp_path, [_task(1, status="todo")])
    result = build_daily_task_snapshot(spark, path, SNAPSHOT_DATE)

    assert len(result) == 1
    row = result.iloc[0]
    assert row["status"] == "todo"
    assert row["task_count"] == 1
    assert row["overdue_count"] == 0


def test_tasks_in_same_group_are_combined(spark, tmp_path):
    path = _write_tasks_parquet(
        tmp_path,
        [
            _task(1, status="todo", owner_id=1, project={"id": 5, "name": "Proj"}),
            _task(2, status="todo", owner_id=1, project={"id": 5, "name": "Proj"}),
        ],
    )
    result = build_daily_task_snapshot(spark, path, SNAPSHOT_DATE)

    assert len(result) == 1
    assert result.iloc[0]["task_count"] == 2


def test_tasks_in_different_groups_produce_separate_rows(spark, tmp_path):
    path = _write_tasks_parquet(tmp_path, [_task(1, status="todo"), _task(2, status="done")])
    result = build_daily_task_snapshot(spark, path, SNAPSHOT_DATE)

    assert len(result) == 2
    assert set(result["status"]) == {"todo", "done"}


def test_task_without_project_uses_sentinel_group(spark, tmp_path):
    path = _write_tasks_parquet(tmp_path, [_task(1, project=None)])
    result = build_daily_task_snapshot(spark, path, SNAPSHOT_DATE)

    row = result.iloc[0]
    assert row["project_id"] == 0
    assert row["project_name"] == "(no project)"


def test_overdue_counts_task_with_past_due_date_and_open_status(spark, tmp_path):
    path = _write_tasks_parquet(tmp_path, [_task(1, status="todo", due_date="2026-07-01")])
    result = build_daily_task_snapshot(spark, path, SNAPSHOT_DATE)
    assert result.iloc[0]["overdue_count"] == 1


def test_overdue_excludes_done_tasks_even_with_past_due_date(spark, tmp_path):
    path = _write_tasks_parquet(tmp_path, [_task(1, status="done", due_date="2026-07-01")])
    result = build_daily_task_snapshot(spark, path, SNAPSHOT_DATE)
    assert result.iloc[0]["overdue_count"] == 0


def test_overdue_excludes_tasks_with_null_due_date(spark, tmp_path):
    path = _write_tasks_parquet(tmp_path, [_task(1, status="todo", due_date=None)])
    result = build_daily_task_snapshot(spark, path, SNAPSHOT_DATE)
    assert result.iloc[0]["overdue_count"] == 0


def test_overdue_excludes_future_due_date(spark, tmp_path):
    path = _write_tasks_parquet(tmp_path, [_task(1, status="todo", due_date="2026-08-01")])
    result = build_daily_task_snapshot(spark, path, SNAPSHOT_DATE)
    assert result.iloc[0]["overdue_count"] == 0


def test_empty_tasks_file_returns_empty_dataframe_with_expected_columns(spark, tmp_path):
    path = _write_tasks_parquet(tmp_path, [])
    result = build_daily_task_snapshot(spark, path, SNAPSHOT_DATE)

    assert result.empty
    assert list(result.columns) == [
        "snapshot_date", "project_id", "project_name",
        "owner_id", "owner_email", "status", "task_count", "overdue_count",
    ]


# ---------------------------------------------------------------------------
# dtype verification after .toPandas() — THE OPEN RISK from ADR-016/017.
# These assert and RECORD the actual type Spark produces, rather than
# assuming it matches pandas_ops.py's behavior.
# ---------------------------------------------------------------------------

def test_snapshot_date_is_a_real_date_or_timestamp_not_a_string(spark, tmp_path):
    """
    clickhouse-connect's Date write path does (x - epoch).days on each
    value (ADR-010) — requires datetime.date or pd.Timestamp, not a
    string or pd.NaT.
    """
    path = _write_tasks_parquet(tmp_path, [_task(1, status="todo")])
    result = build_daily_task_snapshot(spark, path, SNAPSHOT_DATE)

    value = result.iloc[0]["snapshot_date"]
    assert isinstance(value, (date, pd.Timestamp))

    as_date = value.date() if isinstance(value, pd.Timestamp) else value
    assert as_date == SNAPSHOT_DATE_OBJ


# ---------------------------------------------------------------------------
# build_raw_tasks
# ---------------------------------------------------------------------------

def test_raw_tasks_mirrors_each_task_as_one_row(spark, tmp_path):
    path = _write_tasks_parquet(tmp_path, [_task(1, status="todo"), _task(2, status="done")])
    result = build_raw_tasks(spark, path)

    assert len(result) == 2
    assert set(result["id"]) == {1, 2}


def test_raw_tasks_flattens_owner_and_project(spark, tmp_path):
    path = _write_tasks_parquet(
        tmp_path,
        [_task(1, owner_id=3, owner_email="dev@example.com", project={"id": 9, "name": "Proj"})],
    )
    result = build_raw_tasks(spark, path)
    row = result.iloc[0]

    assert row["owner_id"] == 3
    assert row["owner_email"] == "dev@example.com"
    assert row["project_id"] == 9
    assert row["project_name"] == "Proj"


def test_raw_tasks_missing_project_is_none_or_nan_not_a_sentinel(spark, tmp_path):
    """
    Unlike daily_task_snapshot's sentinel, raw_tasks must mirror the
    source faithfully (ADR-012) — no (0, "(no project)"). Whether Spark
    produces literal None or NaN/pd.NA is logged, not assumed:
    load_raw_tasks()'s _normalize_nullable_columns() already handles
    both, but this test makes the actual behavior explicit rather than
    silently relying on that safety net.
    """
    path = _write_tasks_parquet(tmp_path, [_task(1, project=None)])
    result = build_raw_tasks(spark, path)
    row = result.iloc[0]

    logger.info(
        "Spark .toPandas() null representation: project_id=%r (%s)",
        row["project_id"], type(row["project_id"]),
    )
    assert row["project_id"] is None or pd.isna(row["project_id"])
    assert row["project_id"] != 0  # must NOT be daily_task_snapshot's sentinel
    assert row["project_name"] is None or pd.isna(row["project_name"])


def test_raw_tasks_due_date_present_is_a_real_date_or_timestamp(spark, tmp_path):
    path = _write_tasks_parquet(tmp_path, [_task(1, due_date="2026-07-01")])
    result = build_raw_tasks(spark, path)
    value = result.iloc[0]["due_date"]

    assert isinstance(value, (date, datetime, pd.Timestamp))


def test_raw_tasks_due_date_missing_is_none_or_nat(spark, tmp_path):
    path = _write_tasks_parquet(tmp_path, [_task(1, due_date=None)])
    result = build_raw_tasks(spark, path)
    value = result.iloc[0]["due_date"]

    assert value is None or pd.isna(value)


def test_raw_tasks_created_at_is_parsed_not_a_string(spark, tmp_path):
    """
    clickhouse-connect's DateTime write path calls x.timestamp() on
    each value — a raw string does not support that (mirrors
    test_pandas_ops.py's equivalent check).
    """
    path = _write_tasks_parquet(tmp_path, [_task(1)])
    result = build_raw_tasks(spark, path)
    created_at = result.iloc[0]["created_at"]

    assert hasattr(created_at, "timestamp")
    created_at.timestamp()  # must not raise


def test_raw_tasks_empty_file_returns_empty_dataframe_with_expected_columns(spark, tmp_path):
    path = _write_tasks_parquet(tmp_path, [])
    result = build_raw_tasks(spark, path)

    assert result.empty
    assert list(result.columns) == [
        "id", "title", "description", "status", "due_date",
        "owner_id", "owner_email", "project_id", "project_name",
        "created_at", "updated_at",
    ]


# ---------------------------------------------------------------------------
# End-to-end: spark_ops output must load via the EXISTING clickhouse
# loader with no load-side changes — this is ADR-016's specific claim,
# checked here empirically rather than assumed.
# ---------------------------------------------------------------------------

def test_spark_raw_tasks_output_loads_via_existing_clickhouse_loader(spark, tmp_path):
    from src.load.clickhouse_loader import load_raw_tasks

    class _FakeClickHouseClient:
        def __init__(self):
            self.commands = []
            self.inserted = []

        def command(self, cmd, parameters=None):
            self.commands.append((cmd, parameters))

        def insert_df(self, table, df):
            self.inserted.append((table, df))

    path = _write_tasks_parquet(
        tmp_path,
        [
            _task(1, project=None),
            _task(2, project={"id": 9, "name": "Proj"}, due_date="2026-07-01"),
        ],
    )
    df = build_raw_tasks(spark, path)

    client = _FakeClickHouseClient()
    load_raw_tasks(client, df)  # must not raise — that's the assertion

    assert len(client.inserted) == 1
