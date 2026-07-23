"""
Loads transformed data into ClickHouse.

Two tables, two different reload strategies:

- raw_tasks: whole-table TRUNCATE + insert on every run; there is
  no partition key to refresh selectively, since there's nothing to
  preserve between runs by design.
- daily_task_snapshot: per-day partition refresh — see ADR-011. Does NOT
  use whole-table truncate, since that would erase previously
  accumulated history (ADR-008).

Nullable columns (raw_tasks.due_date, .project_id, .project_name):
clickhouse-connect's write path checks `x is None` (and, for numeric
types, `if x`) directly — pd.NaT/pd.NA/np.nan are NOT recognized as null
and either silently produce a wrong (non-null) bit or crash outright.
transform.pandas_ops.build_raw_tasks() already converts all missing
values to literal None before handing off a DataFrame here — see
ADR-012. Date/DateTime columns also require real date/datetime objects,
not strings — clickhouse-connect calls (x - epoch).days / x.timestamp()
directly on each value.
"""
import logging
import os

import clickhouse_connect
import pandas as pd

logger = logging.getLogger(__name__)

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_HTTP_PORT = int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")

DAILY_SNAPSHOT_TABLE = "daily_task_snapshot"
RAW_TASKS_TABLE = "raw_tasks"

_NULLABLE_RAW_TASKS_COLUMNS = ["due_date", "project_id", "project_name"]

_CREATE_DAILY_SNAPSHOT_SQL = f"""
CREATE TABLE IF NOT EXISTS {DAILY_SNAPSHOT_TABLE}
(
    snapshot_date   Date,
    project_id      UInt64,
    project_name    String,
    owner_id        UInt64,
    owner_email     String,
    status          LowCardinality(String),
    task_count      UInt32,
    overdue_count   UInt32
)
ENGINE = MergeTree
PARTITION BY snapshot_date
ORDER BY (snapshot_date, project_id, owner_id, status)
"""

_CREATE_RAW_TASKS_SQL = f"""
CREATE TABLE IF NOT EXISTS {RAW_TASKS_TABLE}
(
    id             UInt64,
    title          String,
    description    String,
    status         LowCardinality(String),
    due_date       Nullable(Date),
    owner_id       UInt64,
    owner_email    String,
    project_id     Nullable(UInt64),
    project_name   Nullable(String),
    created_at     DateTime,
    updated_at     DateTime
)
ENGINE = MergeTree
ORDER BY id
"""


def get_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_HTTP_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )


def ensure_daily_snapshot_table(client) -> None:
    client.command(_CREATE_DAILY_SNAPSHOT_SQL)


def ensure_raw_tasks_table(client) -> None:
    client.command(_CREATE_RAW_TASKS_SQL)


def _drop_partition(client, snapshot_date) -> None:
    """
    Drops the partition for snapshot_date, making reload idempotent per
    day. A no-op if the partition doesn't exist yet (first run for that
    day) — ClickHouse's DROP PARTITION does not error in that case.
    """
    client.command(
        f"ALTER TABLE {DAILY_SNAPSHOT_TABLE} DROP PARTITION %(snapshot_date)s",
        parameters={"snapshot_date": snapshot_date},
    )


def load_daily_task_snapshot(client, df: pd.DataFrame, snapshot_date) -> None:
    """
    Loads df (from transform.pandas_ops.build_daily_task_snapshot) into
    ClickHouse, refreshing only the partition for snapshot_date.

    A no-op beyond ensure_daily_snapshot_table if df is empty — DROP
    PARTITION plus an empty insert would be a wasted round-trip for a
    day where extract legitimately found zero tasks.
    """
    ensure_daily_snapshot_table(client)

    if df.empty:
        logger.info("No rows to load for snapshot_date=%s — skipping", snapshot_date)
        return

    _drop_partition(client, snapshot_date)
    client.insert_df(DAILY_SNAPSHOT_TABLE, df)
    logger.info(
        "Loaded %d row(s) into %s for snapshot_date=%s",
        len(df), DAILY_SNAPSHOT_TABLE, snapshot_date,
    )


def _normalize_nullable_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """
    Replaces pd.NA (or any pandas missing-value sentinel: pd.NaT, np.nan)
    with literal None in the given columns.

    Needed because the DAG's transform task writes build_raw_tasks()'s
    output to an intermediate parquet file (per the project's
    no-DataFrame-via-XCom convention) and the load task reads it back —
    but pandas' own parquet write+read round-trip turns real None into
    pd.NA. clickhouse-connect's Nullable-column write path only
    recognizes literal `None` (see ADR-012); pd.NA is silently treated
    as non-null rather than raising, which is the more dangerous failure
    mode. Applied only to raw_tasks' three genuinely nullable columns,
    not blindly to every column.

    Built via an explicit dtype="object" Series constructor, not bare
    Series.apply() — the same int-to-float upcast bug as ADR-012's
    _flatten_tasks fix, in a new disguise: .apply() returning a mix of
    int + None on an int64[pyarrow]-backed source column (project_id,
    after the parquet round-trip) silently upcasts the whole result to
    float64/NaN. date32[day][pyarrow]-backed columns (due_date) don't
    hit this specific path, but forcing dtype="object" uniformly is
    cheap and removes the need to reason about it per column.
    """
    df = df.copy()
    for col in columns:
        df[col] = pd.Series(
            [None if pd.isna(v) else v for v in df[col]],
            index=df.index,
            dtype="object",
        )
    return df


def load_raw_tasks(client, df: pd.DataFrame) -> None:
    """
    Loads df (from transform.pandas_ops.build_raw_tasks, typically
    reconstituted from an intermediate parquet file — see
    _normalize_nullable_columns) into ClickHouse, replacing the entire
    table's contents — raw_tasks mirrors current state only,
    there is no history to preserve between runs.

    A no-op beyond ensure_raw_tasks_table if df is empty (extract found
    zero tasks) — TRUNCATE still runs, since "TaskTracker currently has
    no tasks" is itself a real state raw_tasks should reflect, not a
    reason to leave stale rows in place.
    """
    ensure_raw_tasks_table(client)

    client.command(f"TRUNCATE TABLE {RAW_TASKS_TABLE}")

    if df.empty:
        logger.info("No tasks to load — %s left empty", RAW_TASKS_TABLE)
        return

    df = _normalize_nullable_columns(df, _NULLABLE_RAW_TASKS_COLUMNS)
    client.insert_df(RAW_TASKS_TABLE, df)
    logger.info("Loaded %d row(s) into %s", len(df), RAW_TASKS_TABLE)
