# Architecture Overview — flowhouse

## Purpose

Batch ETL pipeline: pulls data out of other pet-projects (starting with
TaskTracker's REST API), transforms/aggregates it, and loads it into
ClickHouse for OLAP-style querying. A deliberately different stack from
the other pet-projects — orchestration (Airflow) and a columnar analytics
DB (ClickHouse), not another CRUD backend.

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow 2.9 (LocalExecutor) |
| Extract source (Phase 1) | TaskTracker REST API (JWT) |
| Transform | pandas (Phase 1) → optional PySpark step (Phase 3) |
| Load target | ClickHouse |
| Airflow metadata DB | PostgreSQL 16 (Airflow's own — never pipeline data) |
| Containerization | Docker + docker-compose |

## Project Layout

```
etl-project/
├── docker-compose.yml     # airflow-postgres, clickhouse, airflow (standalone)
├── Dockerfile             # apache/airflow base + our extra deps
├── requirements.txt
├── dags/
│   └── health_check.py    # skeleton — DAG shape only, no real ETL yet
├── src/
│   ├── extract/           # one module per data source (empty so far)
│   ├── transform/         # pandas ops, later spark_ops.py
│   └── load/               # clickhouse_loader.py (not written yet)
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

Phase 3 (a PySpark variant of the transform step) is next — see AGENTS.md.

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
    │      — per-day partition refresh into daily_task_snapshot (ADR-011)
    │
    └──► transform_raw_task → load_raw_task
           (pandas_ops.build_raw_tasks)
           — whole-table truncate + insert into raw_tasks
```

## Integration with TaskTracker

TaskTracker runs in its own docker-compose stack, entirely independent of
this one. The Airflow container reaches it via `host.docker.internal`.
