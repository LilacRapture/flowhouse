"""
Skeleton DAG for the TaskTracker -> ClickHouse ETL pipeline.

This DAG does not extract/transform/load any real data — it only
confirms that both external dependencies (TaskTracker's API, ClickHouse)
are reachable from inside the Airflow container. Kept as a permanent,
lightweight diagnostic DAG alongside sync_tasktracker_to_clickhouse.py
(see ADR-013) — it answers "is anything even reachable?" faster and
with no side effects, distinct from the full pipeline's own failure
modes.
"""
import logging
import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

TASKTRACKER_BASE_URL = os.environ.get("TASKTRACKER_BASE_URL", "http://host.docker.internal:8000")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_HTTP_PORT = int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")


def check_tasktracker() -> None:
    """
    Confirms TaskTracker is reachable. Hits drf-spectacular's schema view,
    which is publicly readable by default — no Airflow Connection/JWT
    needed yet. The real extractor (Phase 1) will authenticate via
    /api/auth/login/ and use an Airflow Connection for credentials.
    """
    url = f"{TASKTRACKER_BASE_URL}/api/schema/"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    logger.info("TaskTracker reachable: %s -> HTTP %s", url, response.status_code)


def check_clickhouse() -> None:
    """Confirms ClickHouse's HTTP interface responds to a trivial query."""
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_HTTP_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )
    result = client.command("SELECT 1")
    logger.info("ClickHouse reachable, SELECT 1 -> %s", result)


default_args = {
    "owner": "nataly",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="health_check",
    description=(
        "Skeleton — verifies TaskTracker's API and ClickHouse are reachable. "
        "Placeholder for the real extract/transform/load pipeline (Phase 1)."
    ),
    default_args=default_args,
    schedule=None,  # manual trigger only; this isn't the real pipeline's schedule
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["skeleton", "health-check"],
) as dag:
    start = EmptyOperator(task_id="start")

    check_tasktracker_task = PythonOperator(
        task_id="check_tasktracker",
        python_callable=check_tasktracker,
    )

    check_clickhouse_task = PythonOperator(
        task_id="check_clickhouse",
        python_callable=check_clickhouse,
    )

    end = EmptyOperator(task_id="end")

    start >> [check_tasktracker_task, check_clickhouse_task] >> end
