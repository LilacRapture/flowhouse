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


def _flatten_tasks(tasks_df: pd.DataFrame, project_field_extractor=_extract_project_fields) -> pd.DataFrame:
    """
    project_id is built with an explicit dtype="object" Series
    constructor, not bare .apply(lambda t: t[0]) — when the extractor can
    return None (the nullable variant used by build_raw_tasks), plain
    .apply() lets pandas infer a Series dtype from the values, and a
    mix of int + None upcasts to float64 + NaN (the same class of bug
    as ADR-006/009, reproduced here in our own code rather than in a
    library). Forcing dtype="object" keeps real ints and real None
    intact. owner_id needs no such handling — owner is never missing.
    """
    owner_fields = tasks_df["owner"].apply(_extract_owner_fields)
    project_fields = tasks_df["project"].apply(project_field_extractor)

    flat = tasks_df.copy()
    flat["owner_id"] = owner_fields.apply(lambda t: t[0])
    flat["owner_email"] = owner_fields.apply(lambda t: t[1])
    flat["project_id"] = pd.Series([t[0] for t in project_fields], index=flat.index, dtype="object")
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


_RAW_TASKS_COLUMNS = [
    "id", "title", "description", "status", "due_date",
    "owner_id", "owner_email", "project_id", "project_name",
    "created_at", "updated_at",
]


def _extract_project_fields_nullable(project):
    """
    Like _extract_project_fields, but returns literal None instead of a
    sentinel — for raw_tasks, which mirrors the source faithfully (see
    ADR-012 on why Nullable columns require real None, not pd.NA).
    """
    if not isinstance(project, dict):
        return None, None
    return project["id"], project["name"]


def _clean_nullable_date_column(series: pd.Series) -> list:
    """
    Converts a string date column to real datetime.date values, with
    missing entries as literal None — clickhouse-connect's Nullable(Date)
    write path checks `x is None` explicitly; pd.NaT (what
    pd.to_datetime(...).dt.date naturally produces for missing values)
    is not recognized as null and crashes the write. See ADR-012.
    """
    parsed = pd.to_datetime(series).dt.date
    return [None if pd.isna(v) else v for v in parsed]


def build_raw_tasks(tasks_path: str) -> pd.DataFrame:
    """
    Reads extract's raw tasks parquet and returns one row per task,
    mirroring the source with minimal transformation (flattened
    owner/project, due_date as a real date-or-None, created_at/updated_at
    as real timestamps) — variant A: current state only, no history.
    Loaded via clickhouse_loader.load_raw_tasks(), which truncates the
    whole table before inserting.

    created_at/updated_at are parsed with pd.to_datetime() — clickhouse-
    connect's DateTime write path calls x.timestamp() on each value, which
    a raw string does not support (pd.Timestamp does). Both fields are
    always present (TaskTracker's auto_now_add/auto_now), so no null
    handling is needed here, unlike due_date/project.
    """
    tasks_df = pd.read_parquet(tasks_path, dtype_backend="pyarrow")

    if tasks_df.empty:
        return pd.DataFrame(columns=_RAW_TASKS_COLUMNS)

    flat = _flatten_tasks(tasks_df, project_field_extractor=_extract_project_fields_nullable)
    flat["due_date"] = _clean_nullable_date_column(flat["due_date"])
    flat["created_at"] = pd.to_datetime(flat["created_at"])
    flat["updated_at"] = pd.to_datetime(flat["updated_at"])

    return flat[_RAW_TASKS_COLUMNS].copy()
    