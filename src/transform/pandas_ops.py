"""
Transform: builds daily_task_snapshot rows from extract's raw tasks
parquet file.

Reads with dtype_backend="pyarrow" (see ADR-009 — amends ADR-006) so
both flat and nested nullable integer columns round-trip correctly.
Every row is grouped by snapshot_date — the DAG's logical run date — not
by any per-task date field (see ADR-008 for why this is a snapshot, not
a created_at cohort).

Nested TaskTracker fields (owner, project) arrive as plain Python dicts
inside an object column; a missing project (TaskTracker's Task.project
is nullable, on_delete=SET_NULL) arrives as pd.NA, not None, under the
pyarrow backend — see ADR-009. owner is never missing (required FK,
on_delete=CASCADE) — see TaskTracker's apps/tasks/models.py.

snapshot_date is stored in the output as a real datetime.date, not a
string — clickhouse-connect's Date-column serializer does
`(value - epoch_start_date).days` internally and only accepts
date/datetime objects; a plain string fails with TypeError at insert
time, not at DataFrame-build time (verified against clickhouse-connect
0.7.19's actual write path — see ADR-010).
"""
import pandas as pd

_NO_PROJECT_ID = 0
_NO_PROJECT_NAME = "(no project)"

_GROUP_COLUMNS = ["project_id", "project_name", "owner_id", "owner_email", "status"]
_OUTPUT_COLUMNS = ["snapshot_date", *_GROUP_COLUMNS, "task_count", "overdue_count"]


def _extract_owner_fields(owner: dict) -> tuple[int, str]:
    return owner["id"], owner["email"]


def _extract_project_fields(project) -> tuple[int, str]:
    if not isinstance(project, dict):
        return _NO_PROJECT_ID, _NO_PROJECT_NAME
    return project["id"], project["name"]


def _flatten_tasks(tasks_df: pd.DataFrame) -> pd.DataFrame:
    owner_fields = tasks_df["owner"].apply(_extract_owner_fields)
    project_fields = tasks_df["project"].apply(_extract_project_fields)

    flat = tasks_df.copy()
    flat["owner_id"] = owner_fields.apply(lambda t: t[0])
    flat["owner_email"] = owner_fields.apply(lambda t: t[1])
    flat["project_id"] = project_fields.apply(lambda t: t[0])
    flat["project_name"] = project_fields.apply(lambda t: t[1])

    return flat


def build_daily_task_snapshot(tasks_path: str, snapshot_date: str) -> pd.DataFrame:
    tasks_df = pd.read_parquet(tasks_path, dtype_backend="pyarrow")

    if tasks_df.empty:
        return pd.DataFrame(columns=_OUTPUT_COLUMNS)

    flat = _flatten_tasks(tasks_df)

    due_date = pd.to_datetime(flat["due_date"])
    snapshot_ts = pd.Timestamp(snapshot_date)
    flat["is_overdue"] = (due_date < snapshot_ts) & (flat["status"] != "done") & due_date.notna()

    grouped = (
        flat.groupby(_GROUP_COLUMNS, dropna=False)
        .agg(task_count=("id", "count"), overdue_count=("is_overdue", "sum"))
        .reset_index()
    )
    grouped.insert(0, "snapshot_date", snapshot_ts.date())

    return grouped[_OUTPUT_COLUMNS]
    