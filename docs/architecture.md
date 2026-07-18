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

**Skeleton stage:** docker-compose + a placeholder `health_check` DAG that
only confirms TaskTracker's API and ClickHouse are reachable from the
Airflow container. No extract/transform/load logic exists yet — see
AGENTS.md for the phase plan.

## Request Lifecycle (once Phase 1 lands)

```
Airflow scheduler triggers DAG (daily)
    │
    ▼
extract_tasks / extract_projects (src/extract/tasktracker.py)
    │   — paginated GET against TaskTracker's API, writes parquet to
    │     a shared volume (not XCom — that's for small metadata only)
    ▼
transform_aggregate (src/transform/pandas_ops.py)
    │   — cleans + aggregates into daily/per-project/per-status stats
    ▼
load_clickhouse (src/load/clickhouse_loader.py)
    — truncate + insert into ClickHouse's daily_task_stats table
```

## Integration with TaskTracker

TaskTracker runs in its own docker-compose stack, entirely independent of
this one. The Airflow container reaches it via `host.docker.internal`,
the same pattern petrag uses to reach Ollama on the host — see
`docs/decisions.md` ADR-002.
