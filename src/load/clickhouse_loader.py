"""
Loads daily_task_snapshot rows into ClickHouse.

Reload strategy: per-day partition refresh, not whole-table truncate.
The table is partitioned by snapshot_date (one partition per day, see
ADR-011), so a re-run for the same day (Airflow retry, manual backfill)
drops just that day's partition before inserting — other days' data is
untouched. This matches ADR-008's decision that daily_task_snapshot
only accumulates forward and never rewrites other days.

Not atomic: DROP PARTITION and the subsequent insert are two separate
statements (ClickHouse has no cross-statement transactions). If insert
fails after a successful drop, that day's partition is left empty until
the next successful run — acceptable here since Airflow's own retry
mechanism is the natural recovery path for an idempotent per-day task.
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

TABLE_NAME = "daily_task_snapshot"

_CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME}
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


def get_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_HTTP_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )


def ensure_table(client) -> None:
    client.command(_CREATE_TABLE_SQL)


def _drop_partition(client, snapshot_date) -> None:
    """
    Drops the partition for snapshot_date, making reload idempotent per
    day. A no-op if the partition doesn't exist yet (first run for that
    day) — ClickHouse's DROP PARTITION does not error in that case.
    """
    client.command(
        f"ALTER TABLE {TABLE_NAME} DROP PARTITION %(snapshot_date)s",
        parameters={"snapshot_date": snapshot_date},
    )


def load_daily_task_snapshot(client, df: pd.DataFrame, snapshot_date) -> None:
    """
    Loads df (from transform.pandas_ops.build_daily_task_snapshot) into
    ClickHouse, refreshing only the partition for snapshot_date.

    client is passed in explicitly (not created internally) — same
    dependency-injection convention as petrag's ingestion/vector_store.py,
    keeps this testable with a fake client and avoids opening a new
    connection per call.

    A no-op beyond ensure_table if df is empty — DROP PARTITION plus an
    empty insert would be a wasted round-trip for a day where extract
    legitimately found zero tasks.
    """
    ensure_table(client)

    if df.empty:
        logger.info("No rows to load for snapshot_date=%s — skipping", snapshot_date)
        return

    _drop_partition(client, snapshot_date)
    client.insert_df(TABLE_NAME, df)
    logger.info(
        "Loaded %d row(s) into %s for snapshot_date=%s", len(df), TABLE_NAME, snapshot_date
    )
    