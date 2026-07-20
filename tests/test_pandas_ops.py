"""
Tests for src/transform/pandas_ops.py. Fixtures write real parquet files
via pyarrow directly (pa.Table.from_pylist + pq.write_table), mirroring
exactly what src/extract/tasktracker.py's _write_parquet() produces —
not a hand-rolled shortcut — so these tests exercise the real
read-path bug surface (see ADR-009) rather than an idealized one.
"""
from datetime import date

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.transform.pandas_ops import (
    _extract_owner_fields,
    _extract_project_fields,
    build_daily_task_snapshot,
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
    due_date: str | None = None,
    owner_id: int = 1,
    owner_email: str = "owner@example.com",
    project: dict | None = None,
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
# _extract_owner_fields
# ---------------------------------------------------------------------------

def test_extract_owner_fields_returns_id_and_email():
    owner = {"id": 7, "email": "dev@example.com", "full_name": "Dev User"}
    assert _extract_owner_fields(owner) == (7, "dev@example.com")


# ---------------------------------------------------------------------------
# _extract_project_fields
# ---------------------------------------------------------------------------

def test_extract_project_fields_returns_id_and_name_when_present():
    project = {"id": 42, "name": "Website Redesign"}
    assert _extract_project_fields(project) == (42, "Website Redesign")


def test_extract_project_fields_returns_sentinel_when_missing():
    """pd.NA under the pyarrow backend — see ADR-009, not None."""

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
# build_daily_task_snapshot — empty input
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


# ---------------------------------------------------------------------------
# build_daily_task_snapshot — snapshot_date stamping
# ---------------------------------------------------------------------------

def test_snapshot_date_is_stamped_on_every_row(tmp_path):
    path = _write_tasks_parquet(
        tmp_path,
        [_task(1, status="todo"), _task(2, status="done")],
    )

    result = build_daily_task_snapshot(path, SNAPSHOT_DATE)

    assert (result["snapshot_date"] == SNAPSHOT_DATE_OBJ).all()
