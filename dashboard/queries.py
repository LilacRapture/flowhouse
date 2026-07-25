"""
Thin ClickHouse query functions for the dashboard service.

Deliberately dependency-light: returns plain Python lists of tuples, not
pandas DataFrames — the dashboard container has no pandas dependency,
so app.py builds Plotly figures directly from these
shapes via go.Scatter / go.Bar rather than plotly.express.

client is passed in explicitly, not created internally by each query
function, keeps these functions easy to test with a fake client.
"""
import os

import clickhouse_connect

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_HTTP_PORT = int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")

DAILY_SNAPSHOT_TABLE = "daily_task_snapshot"
RAW_TASKS_TABLE = "raw_tasks"


def get_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_HTTP_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )


def get_overdue_trend(client) -> list[tuple]:
    """
    One (snapshot_date, total_overdue_count) row per day, summed across
    every project/owner/status combination for that day. Powers the
    "overdue trend over time" line chart.
    """
    result = client.query(
        f"""
        SELECT snapshot_date, sum(overdue_count) AS total_overdue
        FROM {DAILY_SNAPSHOT_TABLE}
        GROUP BY snapshot_date
        ORDER BY snapshot_date
        """
    )
    return result.result_rows


def get_task_count_by_project_status(client) -> list[tuple]:
    """
    One (project_name, status, task_count) row per project/status
    combination, from the CURRENT state in raw_tasks (not a historical
    aggregate — raw_tasks is whole-table-replaced on every DAG run, see
    src/load/clickhouse_loader.py). Powers the "task count by
    project/status" bar chart.

    project_name is coalesced to a literal '(no project)' label in SQL,
    not left as ClickHouse NULL — the bar chart needs a plain string
    category to plot against, and doing it in SQL avoids a second
    None-handling branch in app.py (see flowhouse ADR-021 on preferring
    explicit handling at the boundary rather than downstream inference).
    """
    result = client.query(
        f"""
        SELECT coalesce(project_name, '(no project)') AS project_name,
               status,
               count() AS task_count
        FROM {RAW_TASKS_TABLE}
        GROUP BY project_name, status
        ORDER BY project_name, status
        """
    )
    return result.result_rows
