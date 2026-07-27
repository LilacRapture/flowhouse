# AGENTS.md — flowhouse

> ETL pipeline: TaskTracker's REST API -> transform (pandas, later PySpark)
> -> ClickHouse. Orchestrated by Airflow. Learning-focused: get hands-on
> with Airflow + a columnar OLAP DB, distinct from the other pet-projects'
> stacks.

## Architecture

See `docs/architecture.md` for the full picture. Short version:

- `dags/` — thin DAG files only; no business logic (see Code Style below)
- `src/extract/` — one module per data source
- `src/transform/` — pandas ops, PySpark variant later
- `src/load/` — ClickHouse loaders

## Rules

- DAG files stay thin — a DAG file wires `PythonOperator`s to functions
  imported from `src/`; it does not contain extraction/transform/load
  logic inline. (The current `health_check.py` skeleton is the one
  exception, since there's no real `src/` module to call yet — replace
  its inline functions with real `src/` imports once Phase 1 starts.)
- Don't use Airflow XCom to pass DataFrames/large payloads between tasks —
  write to the shared volume (parquet) instead; XCom is for small
  metadata only.
- Credentials (TaskTracker login, ClickHouse if ever auth-enabled) go
  through Airflow Connections, never hardcoded in DAG/src files.
- Non-obvious decisions get an ADR in `docs/decisions.md`.
- No `print()` for debugging — use `logging` (tasks show up in the
  Airflow UI's per-task logs either way).
- Test philosophy: prefer concrete fake objects over `MagicMock` where a
  fake is cheap to write. Exception: mocking `requests.Session` in
  extractor tests uses `MagicMock` — see ADR-007.

## Status

### Skeleton — Done
- [x] docker-compose (airflow-postgres, clickhouse, airflow/standalone)
- [x] `health_check` DAG — confirms TaskTracker API + ClickHouse reachable
- [x] DAG-import test (`tests/test_health_check_dag.py`)
- [x] Verified end-to-end locally: both `check_tasktracker` and
      `check_clickhouse` pass (see ADR-004 for the ClickHouse
      dedicated-user fix needed to get there)

### Phase 1 — Done
- [x] `src/extract/tasktracker.py` — JWT login via Airflow Connection,
      paginated pull of `/api/tasks/` + `/api/projects/`, write parquet
      (tested end-to-end with mocked HTTP, see `tests/test_extract_tasktracker.py`)
- [x] `src/transform/pandas_ops.py` — daily/per-project/per-status
      aggregates
- [x] `src/load/clickhouse_loader.py` — two tables: `raw_tasks`
      (MergeTree, whole-table TRUNCATE+insert, mirrors current state)
      and `daily_task_snapshot` (MergeTree, PARTITION BY snapshot_date,
      per-day partition refresh)
- [x] `dags/sync_tasktracker_to_clickhouse.py` — 5-task DAG (extract →
      [transform_snapshot → load_snapshot, transform_raw → load_raw]).
      `health_check.py` kept as a separate diagnostic DAG, not replaced
      — see ADR-013. `extract_projects()` exists and is tested but not
      called from this DAG — project data arrives nested in each task,
      a separate projects.parquet is currently unused

### Phase 2 — Done
- [x] Tests for transform (sample -> expected aggregates) — 25 tests in
       `tests/test_pandas_ops.py`
  [x] Tests for load with a fake client — 15 tests in
       `tests/test_clickhouse_loader.py`
  [x] Tests for load against a REAL ClickHouse instance —
       `tests/test_clickhouse_integration.py` (DDL validity, DROP
       PARTITION reload/idempotency, whole-table replace for raw_tasks)
  [x] CI (GitHub Actions): ruff, DAG-import test (part of the normal
       pytest run), ClickHouse service container — see ADR-015

### Phase 3 — Done
- [x] `src/transform/spark_ops.py` — PySpark variant, mirrors pandas_ops.py
      exactly (grouping, overdue definition, output schema); selectable
      per run via `TRANSFORM_ENGINE` env var, local SparkSession
- [x] Explicit pyarrow schema for tasks parquet, needed for Spark struct
      extraction
- [x] ruff upgraded 0.6.9 → 0.15.22
- [x] `tests/test_spark_ops.py` — mirrors test_pandas_ops.py's coverage,
      plus dtype checks after .toPandas() and an end-to-end check that
      Spark output loads via the existing (unmodified) clickhouse_loader

### Phase 4 — Done
- [x] `dashboard/` — standalone Dash + Plotly service, reads
      ClickHouse directly, no changes to Airflow container or pipeline code
- [x] Two charts: overdue trend over time, task count by project/status
- [x] HTTP Basic Auth via a plain Flask `before_request` hook (no
      dash-auth dependency)
- [x] `dashboard/tests/` — auth boundary tests + query-layer tests
      against a fake ClickHouse client, own `pyproject.toml`/`requirements.txt`

### Phase 4.1 — Done
- [x] Interactive project filter (`dcc.Dropdown` + one `@app.callback`)
      added on top of the task-count chart. Motivated by
      giving the `flowhouse-e2e` Playwright suite a real user action to
      drive, not just static-content presence.
- [x] `queries.get_distinct_projects()` + unit tests
      (`dashboard/tests/test_dashboard_filter.py`,
      `test_queries.py::test_get_distinct_projects_*`)\

### Open Questions
- Incremental loads (via `updated_at`) deferred — MVP is full-refresh.
  Revisit if TaskTracker's API grows an `updated_at` filter or data
  volume stops being trivial.
- `_TASKS_ARROW_SCHEMA` (src/extract/tasktracker.py) and the mirrored
  schema in tests/test_spark_ops.py must be kept in sync with
  TaskTracker's TaskSerializer fields — no automated check for drift
  (see ADR-018 consequences).
- Single source-of-truth schema (option B from the ADR-021 retrospective) —
  define the task shape once (e.g., a pydantic model or dataclass) and
  derive both `_TASKS_ARROW_SCHEMA` and the test fixture schema from it,
  instead of maintaining two hand-written copies. Not started — the risk
  is currently hypothetical (no drift incident has happened), and this
  is a real refactor (introduces a new dependency if pydantic isn't
  already in use, touches extract + tests). Worth doing if: a third
  place starts needing the same schema, TaskTracker's Task fields change
  more than rarely, or an actual drift bug occurs.
- Polars as a third transform engine — considered, not started. Unlike
  pandas→pyspark (single-node vs distributed, genuinely different
  paradigm), polars is philosophically closer to pandas (single-process
  DataFrame API). Worth doing as a partial (one function, not fully
  mirrored like ADR-016's pandas/spark symmetry) demonstration rather
  than a third fully-symmetric engine — full symmetry would mean a
  third test file, a third DAG branch, and a third ADR for a skill that
  overlaps heavily with pandas.
- Dashboard has no charts beyond the initial two (overdue trend, task
  count by project/status) — more views (e.g. task velocity, per-owner
  breakdown) would need new `queries.py` functions but no architectural
  change; deferred until a real need for a specific view comes up.