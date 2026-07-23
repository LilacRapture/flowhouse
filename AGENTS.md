# AGENTS.md — flowhouse

> ETL pipeline: TaskTracker's REST API -> transform (pandas, later PySpark)
> -> ClickHouse. Orchestrated by Airflow. Learning-focused: get hands-on
> with Airflow + a columnar OLAP DB, distinct from the other pet-projects'
> stacks.

## Architecture

See `docs/architecture.md` for the full picture. Short version:

- `dags/` — thin DAG files only; no business logic (see Code Style below)
- `src/extract/` — one module per data source
- `src/transform/` — pandas ops (Phase 1), PySpark variant later (Phase 3)
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

### Phase 2 — Not started
- [x] Tests for transform (sample -> expected aggregates) — 25 tests in
       `tests/test_pandas_ops.py`
  [x] Tests for load with a fake client — 15 tests in
       `tests/test_clickhouse_loader.py`
  [x] Tests for load against a REAL ClickHouse instance —
       `tests/test_clickhouse_integration.py` (DDL validity, DROP
       PARTITION reload/idempotency, whole-table replace for raw_tasks)
  [x] CI (GitHub Actions): ruff, DAG-import test (part of the normal
       pytest run), ClickHouse service container — see ADR-015

### Phase 3 — Not started
- [ ] PySpark variant of the transform step (local `SparkSession`, no
      cluster) — added only after Phase 1's pandas pipeline works
      end-to-end

### Open Questions
- Incremental loads (via `updated_at`) deferred — MVP is full-refresh.
  Revisit if TaskTracker's API grows an `updated_at` filter or data
  volume stops being trivial.
