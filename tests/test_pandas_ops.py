from datetime import date

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.transform.pandas_ops import (
    _extract_owner_fields,
    _extract_project_fields,
    build_daily_task_snapshot,
    build_raw_tasks,
)

SNAPSHOT_DATE = "2026-07-15"
SNAPSHOT_DATE_OBJ = date(2026, 7, 15)


def _write_tasks_parquet(tmp_path, records: list[dict]) -> str:
    path = str(tmp_path / "tasks_test.parquet")
    table = pa.Table.from_pylist(records)
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
# _extract_owner_fields / _extract_project_fields
# ---------------------------------------------------------------------------

def test_extract_owner_fields_returns_id_and_email():
    owner = {"id": 7, "email": "dev@example.com", "full_name": "Dev User"}
    assert _extract_owner_fields(owner) == (7, "dev@example.com")


def test_extract_project_fields_returns_id_and_name_when_present():
    project = {"id": 42, "name": "Website Redesign"}
    assert _extract_project_fields(project) == (42, "Website Redesign")


def test_extract_project_fields_returns_sentinel_when_missing():
    assert _extract_project_fields(pd.NA) == (0, "(no project)")


def test_extract_project_fields_returns_sentinel_for_none_too():
    """Defensive: also handle plain None, in case the backend ever changes."""
    assert _extract_project_fields(None) == (0, "(no project)")


# ---------------------------------------------------------------------------
# build_daily_task_snapshot — grouping
# ---------------------------------------------------------------------------

def test_single_task_produces_single_row(tmp_path):
    path = _write_tasks_parquet(tmp_path, [_task(1, status="todo")])
    result = build_daily_task_snapshot(path, SNAPSHOT_DATE)
    assert len(result) == 1
    row = result.iloc[0]
    assert row["snapshot_date"] == SNAPSHOT_DATE_OBJ
    assert row["status"] == "todo"
    assert row["task_count"] == 1
    assert row["overdue_count"] == 0


def test_tasks_in_same_group_are_combined(tmp_path):
    path = _write_tasks_parquet(
        tmp_path,
        [
            _task(1, status="todo", owner_id=1, project={"id": 5, "name": "Proj"}),
            _task(2, status="todo", owner_id=1, project={"id": 5, "name": "Proj"}),
        ],
    )
    result = build_daily_task_snapshot(path, SNAPSHOT_DATE)
    assert len(result) == 1
    assert result.iloc[0]["task_count"] == 2


def test_tasks_in_different_groups_produce_separate_rows(tmp_path):
    path = _write_tasks_parquet(tmp_path, [_task(1, status="todo"), _task(2, status="done")])
    result = build_daily_task_snapshot(path, SNAPSHOT_DATE)
    assert len(result) == 2
    assert set(result["status"]) == {"todo", "done"}


def test_task_without_project_uses_sentinel_group(tmp_path):
    path = _write_tasks_parquet(tmp_path, [_task(1, project=None)])
    result = build_daily_task_snapshot(path, SNAPSHOT_DATE)
    row = result.iloc[0]
    assert row["project_id"] == 0
    assert row["project_name"] == "(no project)"


def test_task_without_project_id_stays_int_not_float(tmp_path):
    path = _write_tasks_parquet(
        tmp_path,
        [
            _task(1, project=None),
            _task(2, project={"id": 7, "name": "Real Project"}),
        ],
    )
    result = build_daily_task_snapshot(path, SNAPSHOT_DATE)
    assert set(result["project_id"]) == {0, 7}
    for value in result["project_id"]:
        assert isinstance(value, int) or float(value).is_integer()


# ---------------------------------------------------------------------------
# build_daily_task_snapshot — overdue_count
# ---------------------------------------------------------------------------

def test_overdue_counts_task_with_past_due_date_and_open_status(tmp_path):
    path = _write_tasks_parquet(tmp_path, [_task(1, status="todo", due_date="2026-07-01")])
    result = build_daily_task_snapshot(path, SNAPSHOT_DATE)
    assert result.iloc[0]["overdue_count"] == 1


def test_overdue_excludes_done_tasks_even_with_past_due_date(tmp_path):
    path = _write_tasks_parquet(tmp_path, [_task(1, status="done", due_date="2026-07-01")])
    result = build_daily_task_snapshot(path, SNAPSHOT_DATE)
    assert result.iloc[0]["overdue_count"] == 0


def test_overdue_excludes_tasks_with_null_due_date(tmp_path):
    path = _write_tasks_parquet(tmp_path, [_task(1, status="todo", due_date=None)])
    result = build_daily_task_snapshot(path, SNAPSHOT_DATE)
    assert result.iloc[0]["overdue_count"] == 0


def test_overdue_excludes_due_date_equal_to_snapshot_date(tmp_path):
    path = _write_tasks_parquet(tmp_path, [_task(1, status="todo", due_date=SNAPSHOT_DATE)])
    result = build_daily_task_snapshot(path, SNAPSHOT_DATE)
    assert result.iloc[0]["overdue_count"] == 0


def test_overdue_excludes_future_due_date(tmp_path):
    path = _write_tasks_parquet(tmp_path, [_task(1, status="todo", due_date="2026-08-01")])
    result = build_daily_task_snapshot(path, SNAPSHOT_DATE)
    assert result.iloc[0]["overdue_count"] == 0


def test_overdue_count_within_a_mixed_group(tmp_path):
    path = _write_tasks_parquet(
        tmp_path,
        [
            _task(1, status="todo", owner_id=1, due_date="2026-07-01"),
            _task(2, status="todo", owner_id=1, due_date="2026-08-01"),
        ],
    )
    result = build_daily_task_snapshot(path, SNAPSHOT_DATE)
    assert len(result) == 1
    assert result.iloc[0]["task_count"] == 2
    assert result.iloc[0]["overdue_count"] == 1


# ---------------------------------------------------------------------------
# build_daily_task_snapshot — empty input / snapshot_date stamping
# ---------------------------------------------------------------------------

def test_empty_tasks_file_returns_empty_dataframe_with_expected_columns(tmp_path):
    path = _write_tasks_parquet(tmp_path, [])
    result = build_daily_task_snapshot(path, SNAPSHOT_DATE)
    assert result.empty
    assert list(result.columns) == [
        "snapshot_date",
        "project_id",
        "project_name",
        "owner_id",
        "owner_email",
        "status",
        "task_count",
        "overdue_count",
    ]


def test_snapshot_date_is_stamped_on_every_row(tmp_path):
    path = _write_tasks_parquet(tmp_path, [_task(1, status="todo"), _task(2, status="done")])
    result = build_daily_task_snapshot(path, SNAPSHOT_DATE)
    assert (result["snapshot_date"] == SNAPSHOT_DATE_OBJ).all()


# ---------------------------------------------------------------------------
# build_raw_tasks
# ---------------------------------------------------------------------------

def test_raw_tasks_mirrors_each_task_as_one_row(tmp_path):
    path = _write_tasks_parquet(
        tmp_path,
        [_task(1, status="todo"), _task(2, status="done")],
    )

    result = build_raw_tasks(path)

    assert len(result) == 2
    assert set(result["id"]) == {1, 2}


def test_raw_tasks_flattens_owner_and_project(tmp_path):
    path = _write_tasks_parquet(
        tmp_path,
        [_task(1, owner_id=3, owner_email="dev@example.com", project={"id": 9, "name": "Proj"})],
    )

    result = build_raw_tasks(path)
    row = result.iloc[0]

    assert row["owner_id"] == 3
    assert row["owner_email"] == "dev@example.com"
    assert row["project_id"] == 9
    assert row["project_name"] == "Proj"


def test_raw_tasks_missing_project_is_literal_none_not_sentinel(tmp_path):
    """
    Unlike build_daily_task_snapshot's sentinel (0/"(no project)"),
    raw_tasks mirrors the source faithfully — missing project must be
    real None (both fields), required for ClickHouse's Nullable columns.
    """
    path = _write_tasks_parquet(tmp_path, [_task(1, project=None)])

    result = build_raw_tasks(path)
    row = result.iloc[0]

    assert row["project_id"] is None
    assert row["project_name"] is None


def test_raw_tasks_missing_project_id_stays_int_or_none_not_float(tmp_path):
    """Regression test: mixing a real project_id with a missing one in
    the same file must not upcast the real id to float (see the
    _flatten_tasks dtype="object" fix, documented alongside ADR-012)."""
    path = _write_tasks_parquet(
        tmp_path,
        [
            _task(1, project=None),
            _task(2, project={"id": 9, "name": "Proj"}),
        ],
    )

    result = build_raw_tasks(path)

    for value in result["project_id"]:
        assert value is None or isinstance(value, int)


def test_raw_tasks_due_date_present_is_a_real_date_object(tmp_path):
    path = _write_tasks_parquet(tmp_path, [_task(1, due_date="2026-07-01")])

    result = build_raw_tasks(path)

    assert isinstance(result.iloc[0]["due_date"], date)


def test_raw_tasks_due_date_missing_is_literal_none_not_nat(tmp_path):
    """
    Regression test for ADR-012: pd.to_datetime(...).dt.date naturally
    produces pd.NaT for missing values, which clickhouse-connect's
    Nullable(Date) write path does NOT recognize as null. Must be real
    None.
    """
    path = _write_tasks_parquet(tmp_path, [_task(1, due_date=None)])

    result = build_raw_tasks(path)

    assert result.iloc[0]["due_date"] is None


def test_raw_tasks_created_at_is_parsed_not_a_string(tmp_path):
    """
    clickhouse-connect's DateTime write path calls x.timestamp() on each
    value — a raw string does not support that.
    """
    path = _write_tasks_parquet(tmp_path, [_task(1)])

    result = build_raw_tasks(path)
    created_at = result.iloc[0]["created_at"]

    assert hasattr(created_at, "timestamp")
    created_at.timestamp()  # must not raise


def test_raw_tasks_empty_file_returns_empty_dataframe_with_expected_columns(tmp_path):
    path = _write_tasks_parquet(tmp_path, [])

    result = build_raw_tasks(path)

    assert result.empty
    assert list(result.columns) == [
        "id", "title", "description", "status", "due_date",
        "owner_id", "owner_email", "project_id", "project_name",
        "created_at", "updated_at",
    ]
