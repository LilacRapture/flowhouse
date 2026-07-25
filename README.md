# flowhouse
![Tests](https://github.com/LilacRapture/flowhouse/actions/workflows/tests.yml/badge.svg)

Batch ETL pipeline: TaskTracker's REST API → transform (pandas, later
PySpark) → ClickHouse. Orchestrated by Apache Airflow.

**Status:** Phases 1–2 complete — the full extract → transform → load
pipeline (`sync_tasktracker_to_clickhouse` DAG) runs end-to-end against a
real ClickHouse instance, with unit + integration test coverage and CI.
Phase 3 (PySpark transform variant) is next — see `AGENTS.md`.

## Stack

- Apache Airflow 2.9 (LocalExecutor, `airflow standalone`)
- ClickHouse
- Python 3.12
- pandas (default transform engine) or PySpark (`TRANSFORM_ENGINE=spark`,
  local SparkSession, no cluster)

## Quick start

Requires Docker and Docker Compose. TaskTracker's own docker-compose stack
should already be running separately (`http://localhost:8000`) if you want
the `check_tasktracker` task to succeed.

```bash
cp .env.example .env   # adjust AIRFLOW_DB_* / TASKTRACKER_BASE_URL if needed
docker compose up --build
```

First run takes a minute — `airflow standalone` initializes its metadata
DB and creates an admin user. Find the generated password with:

```bash
docker compose logs airflow | grep -A1 "Password for user 'admin'"
```

Airflow UI: `http://localhost:8080` (not to be confused with TaskTracker's
own `http://localhost:8000`).

Trigger the skeleton DAG manually (it has no schedule):

```bash
docker compose exec airflow airflow dags trigger health_check
```

Then check its run in the UI, or:

```bash
docker compose exec airflow airflow dags list-runs -d health_check
```

## Transform engine

The `sync_tasktracker_to_clickhouse` DAG's transform step defaults to
pandas. Set `TRANSFORM_ENGINE=spark` in `.env` to run the same logic via
a local PySpark session instead — output schema is identical either way,
so the load step and ClickHouse tables don't change.

## Tests

```bash
docker compose exec airflow pytest /opt/airflow/tests
```

(Needs to run inside the container, where Airflow itself is installed.)

## Documentation

| File | Purpose |
|------|---------|
| [AGENTS.md](AGENTS.md) | Conventions + phase-by-phase status |
| [docs/architecture.md](docs/architecture.md) | System overview |
| [docs/decisions.md](docs/decisions.md) | Architecture decision records |
