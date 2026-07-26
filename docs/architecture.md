# Architecture Overview — flowhouse

## Purpose

Batch ETL pipeline: pulls data out of other pet-projects (starting with
TaskTracker's REST API), transforms/aggregates it, and loads it into
ClickHouse for OLAP-style querying.

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow 2.9 (LocalExecutor) |
| Extract source | TaskTracker REST API (JWT) |
| Transform | pandas → optional PySpark step |
| Load target | ClickHouse |
| Visualization | Dash + Plotly (separate service, HTTP Basic Auth) |
| Airflow metadata DB | PostgreSQL 16 (Airflow's own — never pipeline data) |
| Containerization | Docker + docker-compose |

## Project Layout

```
flowhouse/
├── docker-compose.yml # airflow-postgres, clickhouse, airflow (standalone), dashboard
├── Dockerfile # apache/airflow base + our extra deps
├── requirements.txt
├── dags/
│ ├── health_check.py # permanent connectivity-only DAG
│ └── sync_tasktracker_to_clickhouse.py # real ETL DAG; TRANSFORM_ENGINE env var
│                                       # selects pandas (default) or spark
├── src/
│ ├── extract/
│ │ └── tasktracker.py # JWT login, paginated pull, parquet write
│ │                    # (explicit pyarrow schema for tasks)
│ ├── transform/
│ │ ├── pandas_ops.py # default transform engine
│ │ └── spark_ops.py  # PySpark variant — same grouping/output
│ │                   # schema as pandas_ops.py, local SparkSession
│ └── load/
│ └── clickhouse_loader.py
├── dashboard/ # Dash + Plotly visualization service (Phase 4)
├── tests/
├── docs/
└── .env.example
```

## Current Status

**Phases 1–2 complete.** The real pipeline (`sync_tasktracker_to_clickhouse`
DAG) extracts tasks from TaskTracker, transforms them with pandas into
`raw_tasks` and `daily_task_snapshot` shapes, and loads both into
ClickHouse — end-to-end, with unit tests, a ClickHouse integration test
suite, and CI (ruff + pytest + a ClickHouse service container). The
`health_check` DAG remains as a separate, permanent connectivity check.

**Phase 3 (PySpark transform engine) complete.** `src/transform/spark_ops.py`
mirrors `pandas_ops.py`'s two functions exactly (same grouping, same
overdue definition, same output schema) as a parallel, selectable engine —
not a replacement. Selected via the `TRANSFORM_ENGINE` env var (`pandas`
default, or `spark`), runs as a local SparkSession inside the same
container (no separate cluster).

**Phase 4 (Dash dashboard) complete.** A separate `dashboard` service
reads `raw_tasks` and `daily_task_snapshot` directly from ClickHouse and
renders two charts (overdue trend over time, task count by
project/status).

## Visualization

A standalone Dash + Plotly service (`dashboard/`), independent of the
Airflow container.

dashboard/
├── app.py # Dash layout + HTTP Basic Auth (plain Flask before_request hook)
├── queries.py # thin ClickHouse query functions (get_overdue_trend, get_task_count_by_project_status)
├── requirements.txt # dash, plotly, clickhouse-connect — no pandas/pyspark/airflow
├── Dockerfile
├── pyproject.toml # own pytest/ruff config, pythonpath = ["."]
└── tests/

Key points:
- Image-only deploy (like TaskTracker's `web` service) —
  not bind-mounted like `airflow`'s `dags`/`src`. The
  dashboard doesn't need the same live-reload workflow Airflow's DAG
  development does; code changes require `docker compose build dashboard`.
- Data is queried fresh from ClickHouse on every page load, not polled
  on an interval — `sync_tasktracker_to_clickhouse` runs `@daily`, 
  so the underlying tables change at most once a day.
- `app.layout` is assigned a callable (`serve_layout`), not a fixed
  component tree — this is what makes "fresh query per page load" work
  without a `dcc.Interval`.
- Protected by HTTP Basic Auth (`DASHBOARD_USER`/`DASHBOARD_PASSWORD` in
  `.env`) — demo-level access control, not a substitute for real auth if
  ever exposed beyond a local/demo host.

## Request Lifecycle

```
Airflow scheduler triggers DAG (daily)
    │
    ▼
extract_tasks_task (src/extract/tasktracker.py)
    │   — paginated GET against TaskTracker's /api/tasks/, writes parquet
    │     to a shared volume (not XCom). extract_projects() exists and is
    │     tested but isn't called — project data already arrives nested
    │     inside each task.
    │
    ├──► transform_snapshot_task → load_snapshot_task
    │      (pandas_ops.build_daily_task_snapshot)
    │      — per-day partition refresh into daily_task_snapshot
    │
    └──► transform_raw_task → load_raw_task
           (pandas_ops.build_raw_tasks)
           — whole-table truncate + insert into raw_tasks

Separately, on demand (not part of the DAG run):

Browser request → dashboard service (dashboard/app.py)
    │   — HTTP Basic Auth check (before_request hook)
    ▼
serve_layout() queries ClickHouse fresh (dashboard/queries.py)
    │   — raw_tasks (task count by project/status)
    │   — daily_task_snapshot (overdue trend over time)
    ▼
Two Plotly charts rendered in the browser
```

## Integration with TaskTracker

TaskTracker runs in its own docker-compose stack, entirely independent of
this one. The Airflow container reaches it via `host.docker.internal`.
