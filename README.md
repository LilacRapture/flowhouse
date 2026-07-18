# flowhouse

Batch ETL pipeline: TaskTracker's REST API → transform (pandas, later
PySpark) → ClickHouse. Orchestrated by Apache Airflow.

**Status:** skeleton only — docker-compose + a placeholder DAG that checks
TaskTracker and ClickHouse are reachable. No real extract/transform/load
logic yet (see `AGENTS.md` for the phase plan).

## Stack

- Apache Airflow 2.9 (LocalExecutor, `airflow standalone`)
- ClickHouse
- Python 3.12

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
