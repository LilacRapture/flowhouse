"""
Real ETL DAG: TaskTracker -> ClickHouse (raw_tasks + daily_task_snapshot).

    extract_tasks_task
        |--> transform_snapshot_task --> load_snapshot_task
        `--> transform_raw_task      --> load_raw_task

extract_projects() (src/extract/tasktracker.py) is deliberately NOT
called here — project data is already embedded in each task's nested
`project` field (TaskTracker's TaskSerializer), so a separate
projects.parquet file would be unused by both transform functions. The
function itself is kept (not deleted) for a possible future
projects-focused report; it's simply not wired into this DAG.

XCom carries only file paths (strings) between tasks, never DataFrames —
each task writes its output to the shared data volume and returns the
path, per project convention (see AGENTS.md).
"""
import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task

from extract.tasktracker import extract_tasks
from load.clickhouse_loader import (
    get_client,
    load_daily_task_snapshot,
    load_raw_tasks,
)
from transform.pandas_ops import build_daily_task_snapshot as _pandas_build_daily_task_snapshot
from transform.pandas_ops import build_raw_tasks as _pandas_build_raw_tasks

# Engine switch — "pandas" (default) or "spark".
# Read once at DAG-parse time from TRANSFORM_ENGINE (docker-compose.yml
# / .env), not a DAG param — changeable without editing/redeploying the
# DAG file itself.
TRANSFORM_ENGINE = os.environ.get("TRANSFORM_ENGINE", "pandas").lower()

logger = logging.getLogger(__name__)

default_args = {
    "owner": "nataly",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def _build_daily_task_snapshot(tasks_parquet_path: str, snapshot_date) -> pd.DataFrame:
    if TRANSFORM_ENGINE == "spark":
        # Imported lazily, not at module top — avoids requiring a JVM
        # at every DAG-file parse (Airflow's scheduler re-parses DAG
        # files on an interval) when pandas is the selected engine.
        from transform.spark_ops import build_daily_task_snapshot as _spark_build
        from transform.spark_ops import get_spark

        spark = get_spark()
        try:
            return _spark_build(spark, tasks_parquet_path, str(snapshot_date))
        finally:
            spark.stop()
    return _pandas_build_daily_task_snapshot(tasks_parquet_path, snapshot_date)


def _build_raw_tasks(tasks_parquet_path: str) -> pd.DataFrame:
    if TRANSFORM_ENGINE == "spark":
        from transform.spark_ops import build_raw_tasks as _spark_build
        from transform.spark_ops import get_spark

        spark = get_spark()
        try:
            return _spark_build(spark, tasks_parquet_path)
        finally:
            spark.stop()
    return _pandas_build_raw_tasks(tasks_parquet_path)


@dag(
    dag_id="sync_tasktracker_to_clickhouse",
    description="Extract TaskTracker tasks, transform, load into ClickHouse (raw_tasks + daily_task_snapshot).",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "tasktracker", "clickhouse"],
)
def sync_tasktracker_to_clickhouse():

    @task
    def extract_tasks_task() -> str:
        return extract_tasks()

    @task
    def transform_snapshot_task(tasks_parquet_path: str, ds: str = None) -> str:
        df = _build_daily_task_snapshot(tasks_parquet_path, ds)
        output_path = tasks_parquet_path.replace("tasks_", "daily_task_snapshot_")
        df.to_parquet(output_path)
        logger.info(
            "Wrote %d snapshot row(s) to %s (engine=%s)",
            len(df), output_path, TRANSFORM_ENGINE,
        )
        return output_path

    @task
    def load_snapshot_task(snapshot_parquet_path: str, ds: str = None) -> None:
        df = pd.read_parquet(snapshot_parquet_path, dtype_backend="pyarrow")
        client = get_client()
        load_daily_task_snapshot(client, df, ds)

    @task
    def transform_raw_task(tasks_parquet_path: str) -> str:
        df = _build_raw_tasks(tasks_parquet_path)
        output_path = tasks_parquet_path.replace("tasks_", "raw_tasks_")
        df.to_parquet(output_path)
        logger.info(
            "Wrote %d raw task row(s) to %s (engine=%s)",
            len(df), output_path, TRANSFORM_ENGINE,
        )
        return output_path

    @task
    def load_raw_task(raw_tasks_parquet_path: str) -> None:
        df = pd.read_parquet(raw_tasks_parquet_path, dtype_backend="pyarrow")
        client = get_client()
        load_raw_tasks(client, df)

    tasks_path = extract_tasks_task()

    snapshot_path = transform_snapshot_task(tasks_path)
    load_snapshot_task(snapshot_path)

    raw_path = transform_raw_task(tasks_path)
    load_raw_task(raw_path)


sync_tasktracker_to_clickhouse()
