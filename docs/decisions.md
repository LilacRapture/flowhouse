# Architecture Decision Records (ADR) — flowhouse

## ADR-001 — LocalExecutor via `airflow standalone`, not CeleryExecutor

**Date:** project start
**Status:** Accepted

**Decision:** Run Airflow with `AIRFLOW__CORE__EXECUTOR=LocalExecutor` using
the `airflow standalone` command (webserver + scheduler + triggerer in one
process/container), backed by a dedicated Postgres for Airflow's own
metadata. No Redis, no separate worker service.

**Context:** The goal of this project is to get hands-on with Airflow
itself. CeleryExecutor's main new concept — a distributed task queue via
Redis — is something already demonstrated elsewhere in the portfolio
(Redis in the URL Shortener project), so it wouldn't add a new skill here,
only more moving parts to run and debug locally.

**Alternatives considered:**
- CeleryExecutor (+Redis +worker) — more "production-like," but the
  portfolio differentiation goal is better served by keeping the new
  surface area focused on Airflow + ClickHouse.
- SequentialExecutor — the true minimal option, but can't run tasks in
  parallel at all (even the two independent health-check tasks in the
  skeleton DAG), which undersells even basic Airflow scheduling.

**Consequences:**
- Single point of failure for the whole Airflow stack (one container) —
  acceptable for a local portfolio demo, would need revisiting for any
  real deployment.
- `airflow standalone` auto-creates an admin user and prints its password
  to the container logs on first run — not meant for anything beyond
  local/dev use.

---

## ADR-002 — Reach TaskTracker via `host.docker.internal`, no shared Docker network

**Date:** project start
**Status:** Accepted

**Decision:** TaskTracker keeps its own docker-compose stack, fully
independent of this project's. The Airflow container reaches TaskTracker's
API at `http://host.docker.internal:8000` (configurable via
`TASKTRACKER_BASE_URL`) rather than joining a shared Docker network across
both repos.

**Context:** Same reasoning as petrag ADR-003: portfolio projects are
intentionally separate repos/stacks to demonstrate range, not one
mega-compose file. This project treats TaskTracker as an external system
it happens to read from — via its public HTTP API — the same way petrag
treats Ollama as an external system on the host.

**Alternatives considered:**
- Shared external Docker network (`docker network create shared-net`,
  both compose files attach to it) — would work, but couples two
  otherwise-independent repos' deployment lifecycles together for no
  real benefit here (unlike, say, wanting SQL-level access to
  TaskTracker's database, which would additionally raise its own
  cross-service data-ownership questions).

**Consequences:**
- Requires TaskTracker's compose stack to be running separately
  (`docker-compose up` in its own repo) before this project's extractor
  can succeed.
- Only works on Docker Desktop / OrbStack, which resolve
  `host.docker.internal` automatically; a Linux host would need the
  `extra_hosts: host.docker.internal:host-gateway` entry (see petrag's
  docker-compose.yml for the same caveat).

---

## ADR-003 — Bind-mount `dags/`, `src/`, `tests/` instead of baking them into the image

**Date:** project start
**Status:** Accepted

**Decision:** `docker-compose.yml` bind-mounts `./dags`, `./src`, and
`./tests` into the Airflow container. The Dockerfile only installs Python
dependencies (`requirements.txt`) — it does not `COPY` our own code.

**Context:** This is the opposite choice from TaskTracker's ADR-013, which
deliberately removed bind mounts so the container runs from the image's
own baked-in code copy. Airflow's normal dev workflow expects the
scheduler to pick up DAG file changes live (it polls `dags/` on an
interval) without a rebuild — that's standard practice in Airflow's own
quick-start docs, not a shortcut specific to this project.

**Consequences:**
- Editing a DAG or a `src/` module takes effect within Airflow's DAG-file
  scan interval — no `docker compose build` needed during development.
- If this project ever needs a "frozen" deployment (e.g. a demo snapshot
  meant to run unmodified), that would warrant switching to an
  image-baked copy, mirroring TaskTracker's approach, and should get its
  own ADR at that point.

---

## ADR-004 — Dedicated ClickHouse user, not `default`

**Date:** skeleton verification
**Status:** Accepted

**Decision:** `docker-compose.yml` sets `CLICKHOUSE_DB` / `CLICKHOUSE_USER` /
`CLICKHOUSE_PASSWORD` / `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1` on the
`clickhouse` service, and all clients (currently just
`check_clickhouse()` in the skeleton DAG) authenticate as that user via
`CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` env vars — never as `default`.

**Context:** First manual test run failed with `AUTHENTICATION_FAILED`
even with no password configured anywhere. Per ClickHouse's own Docker
docs, the `default` user's network access is disabled outright unless at
least one of `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, or
`CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT` is set — so leaving all three
unset (the skeleton's original state) doesn't mean "open/no-auth," it
means "no client can authenticate over the network at all."

**Alternatives considered:**
- `CLICKHOUSE_SKIP_USER_SETUP=1` — makes `default` reachable with no
  password. Simpler, but leaves a literally unauthenticated user open on
  the network; a named user with a password (even a throwaway one for
  local dev) is a small cost for not normalizing "no auth" as the
  project's default.

**Consequences:**
- Credentials live in `.env` (`CLICKHOUSE_USER`/`CLICKHOUSE_PASSWORD`),
  same convention as `AIRFLOW_DB_*`.
- Any future loader (`src/load/clickhouse_loader.py`) must read these
  same two env vars rather than connecting as `default`.

---

## ADR-005 — TaskTracker credentials via env-var Airflow Connection

**Date:** Phase 1 start
**Status:** Accepted

**Decision:** Store TaskTracker's login credentials as an Airflow
Connection defined via the `AIRFLOW_CONN_TASKTRACKER_API` env var (URI
format), set in `.env` — not created manually through the Airflow UI, and
not a bespoke `TASKTRACKER_USER`/`TASKTRACKER_PASSWORD` pair read
directly by our own code.

**Context:** Needed a way to store TaskTracker credentials that (a)
doesn't hardcode secrets, (b) doesn't require a manual UI step on every
fresh environment, and (c) uses Airflow's own secrets mechanism rather
than reinventing it. Env-var Connections satisfy all three — Airflow
parses `AIRFLOW_CONN_<CONN_ID>` at startup and it's retrievable the same
way as a UI-created one (`BaseHook.get_connection("tasktracker_api")`).

**Alternatives considered:**
- Manual Connection via Airflow UI — standard Airflow practice, but a
  manual step that's easy to forget when recreating an environment; not
  reproducible from a fresh `.env`.
- Plain `TASKTRACKER_USER`/`TASKTRACKER_PASSWORD` env vars read directly
  by `src/extract/tasktracker.py` — simplest, but sidesteps Airflow's
  built-in credential storage/masking for no real benefit.

**Consequences:**
- TaskTracker logs in via **email**, which contains `@` — and `@` is
  already the URI's separator between userinfo and host. The email must
  be percent-encoded (`%40`) in the env var or the Connection parses
  incorrectly. Documented directly in `.env.example`.
- `src/extract/tasktracker.py` will retrieve
  this via `BaseHook.get_connection("tasktracker_api")` and use
  `.login` / `.password` as the email/password pair for
  `POST /api/auth/login/` — not as HTTP Basic Auth headers, since
  TaskTracker's login endpoint expects a JSON body, not a `Connection`
  object is just a generic credential container here.

---

## ADR-006 — Write parquet via pyarrow directly, not pandas.to_parquet()

**Date:** Phase 1
**Status:** Accepted

**Decision:** `src/extract/tasktracker.py`'s `_write_parquet()` builds a
`pyarrow.Table` from the raw records and writes it with
`pyarrow.parquet.write_table()` — it does not go through
`pandas.DataFrame.to_parquet()`.

**Context:** `pd.DataFrame.from_records()` silently upcasts integer
columns containing `None` to `float64` (confirmed: `{"id": 5}` /
`{"id": None}` in the same column → `5.0` / `nan`, not `5` / `null`).
Extract's job is to persist raw data unmodified — introducing pandas'
type-coercion quirks at this stage, before any real processing happens,
was an avoidable source of silent data corruption for nullable fields
(e.g. a future flat nullable FK id).

**Alternatives considered:**
- `pd.DataFrame.to_parquet(engine="pyarrow")` — works, and pandas does
  use pyarrow under the hood regardless, but still round-trips through a
  pandas DataFrame first (extra conversion, and the upcast footgun above)
  for no benefit — extract does no DataFrame-specific operations at all.

**Consequences:**
- The parquet file itself correctly preserves `int64` + null (verified
  directly via `pyarrow.parquet.read_table()`).
- **This alone does not fully solve the problem** — `pandas.read_parquet()`
  with its default settings *still* upcasts int64+null back to `float64`
  on the way in, regardless of how the file was written. **Amended by
  ADR-009:** the originally documented fix here (`dtype_backend=
  "numpy_nullable"`) turned out to have the same upcast bug one level
  deeper, for nullable *nested struct* fields (`owner`, `project`) —
  see ADR-009 for the correct read-side setting.

---

## ADR-007 — MagicMock for requests.Session in extractor tests (exception to fake-object preference)

**Date:** Phase 1
**Status:** Accepted

**Decision:** `tests/test_extract_tasktracker.py` mocks `requests.Session`
with `unittest.mock.MagicMock`, not a hand-written fake session object.
The Airflow `Connection` object it also depends on IS faked (`FakeConn`).

**Context:** The project convention (established in TaskTracker/petrag)
prefers concrete fakes over `MagicMock`. `requests.Session` here is used
through exactly two thin calls (`.post(...).json()`, `.get(...).json()`)
plus attribute assignment (`session.headers[...] = ...`) — there's no
meaningful behavior to fake beyond "return this canned response," and
`MagicMock`'s `assert_called_once_with(...)` already gives a stricter
call-shape check than a hand-rolled fake would for free.

**Alternatives considered:**
- A `_FakeSession` class with a queue of canned responses (mirroring
  `FakeConn`) — fully consistent with the stated convention, but adds
  boilerplate for an object with no real logic to fake; the convention's
  underlying goal (avoid `MagicMock`'s silent-typo/over-permissive
  interface) isn't really at stake for a 2-method HTTP client shim.

**Consequences:**
- This is a deliberate, narrow exception — not a reopening of the
  fake-vs-MagicMock question generally. New tests elsewhere in this
  project (transform, load) should still default to concrete fakes
  unless they hit the same "thin external HTTP/SDK client, no logic to
  fake" shape as here.

---

## ADR-008 — Daily snapshot by DAG run date, not cohort by created_at

**Date:** Phase 1
**Status:** Accepted

**Decision:** `daily_task_snapshot` groups rows by `snapshot_date` — the
Airflow DAG's logical run date (same date embedded in extract's parquet
filename) — not by each task's `created_at::date`.

**Context:** Load strategy is full-refresh (truncate + insert) per
`src/load/__init__.py`. Grouping by `created_at::date` instead would mean
every run recomputes ALL historical rows from the tasks' current state —
a task created weeks ago that changes status today silently rewrites its
historical row instead of the table gaining a new one. That's a
misleading shape for a table named "daily stats": readers would
reasonably assume past rows are stable facts, not a re-derived view.
Grouping by run date instead makes each run append one new day of rows
and never touch previous days — the standard shape for a daily snapshot
fact table, and a clearer illustration of the ETL pattern this project
exists to practice.

**Alternatives considered:**
- Cohort by `created_at::date` — analytically richer per run (backfills
  history immediately), but semantically misleading under full-refresh
  and a poor fit for what "daily snapshot" implies.

**Consequences:**
- History only accumulates going forward — no data exists for dates
  before the pipeline started running (acceptable, no backfill
  requirement exists for this portfolio project).
- Table renamed `daily_task_snapshot` (was `daily_task_stats` in early
  planning docs) to avoid the "stats" name implying event-based
  aggregation.
- Sets up cleanly for future incremental-load ideas (Open Questions in
  AGENTS.md) — snapshots are naturally append-only, nothing to redo.

---

## ADR-009 — dtype_backend="pyarrow" for reading parquet in transform (amends ADR-006)

**Date:** Phase 1
**Status:** Accepted

**Decision:** `src/transform/pandas_ops.py` reads extract's tasks parquet
with `pd.read_parquet(path, dtype_backend="pyarrow")`, not
`dtype_backend="numpy_nullable"` as originally documented in ADR-006.

**Context:** ADR-006 established that writing must go through pyarrow
directly to avoid pandas silently upcasting `int64` + `None` columns to
`float64`, and recommended reading back with `dtype_backend=
"numpy_nullable"` to preserve that. Verified empirically while building
the transform step: `numpy_nullable` (and the plain default backend)
still upcasts an `int64` child field to `float64` inside a **nested
struct column** (e.g. `project: struct<id: int64, name: string>`) as
soon as any row has a null struct (`project=None` — TaskTracker's
`Task.project` is nullable, `SET_NULL`). Confirmed with a real parquet
file: reading `project.id` came back as `100.0`, while
`pyarrow.parquet.read_table(...).to_pylist()` on the exact same file
correctly returned `100` (int) — the corruption is introduced by
pandas' struct-to-Python conversion, not present in the underlying data.
`dtype_backend="pyarrow"` avoids this entirely — verified both for the
nested-struct case (stays `int64`) and for flat nullable-int columns
(the scenario ADR-006 originally cared about).

**Alternatives considered:**
- Read nested `owner`/`project` columns via raw `pyarrow` (bypassing
  pandas for just those columns), keep `numpy_nullable` for the rest —
  works, but means two different read mechanisms for one file and two
  schemas to keep in sync, for no benefit over just switching the
  backend.
- Manually cast `int(project["id"])` after extraction — fixes the
  symptom, not the mechanism ADR-006 was specifically written to avoid;
  rejected for being the same class of silent-coercion bug ADR-006
  already decided against, one level deeper.

**Consequences:**
- Missing nested values arrive as `pd.NA`, not `None` — code checks
  `isinstance(value, dict)` rather than `is None` / `pd.isna()`.
- Numeric/aggregate columns produced by pandas operations on
  `pyarrow`-backed input come back as Arrow-backed dtypes (e.g.
  `int64[pyarrow]`) rather than plain numpy — noted for
  `src/load/clickhouse_loader.py`, which should confirm
  `clickhouse-connect`'s `insert_df()` handles these directly (not yet
  verified — flag for Phase 1's load step).
- `src/transform/__init__.py`'s module docstring updated to point to
  `dtype_backend="pyarrow"` instead of `"numpy_nullable"`.

---

## ADR-010 — snapshot_date as datetime.date, not str, in transform's output

**Date:** Phase 1
**Status:** Accepted

**Decision:** `build_daily_task_snapshot()` stamps `snapshot_date` as a
real `datetime.date` object (`pd.Timestamp(snapshot_date).date()`), not
the raw ISO string passed in.

**Context:** Verified against clickhouse-connect 0.7.19's actual source
(`datatypes/temporal.py`, `Date._write_column_binary`): the Date-column
serializer computes `(value - epoch_start_date).days` directly on each
column value, requiring a `date`/`datetime` object. A plain string
passes through `_convert_pandas()` untouched (its datetime-specific
branch only triggers for actual datetime dtypes) and only fails later,
inside `_write_column_binary`, at actual insert time — i.e. the bug
would not have surfaced until `clickhouse_loader.py` ran against a real
ClickHouse instance, not during transform's own tests.

**Consequences:**
- `src/load/clickhouse_loader.py` can rely on `snapshot_date` already
  being a proper `date` — no conversion needed on the load side.
- General principle reinforced (same as ADR-006/ADR-009): fix data
  typing as early in the pipeline as possible, don't defer correctness
  to whichever step happens to fail first.

---

## ADR-011 — Per-day partition refresh for daily_task_snapshot, not whole-table truncate

**Date:** Phase 1
**Status:** Accepted

**Decision:** `daily_task_snapshot` is a `MergeTree` table with
`PARTITION BY snapshot_date` (one partition per day). Loading refreshes
only the partition matching the run's `snapshot_date` — via
`ALTER TABLE ... DROP PARTITION` followed by `insert_df()` — not a
whole-table `TRUNCATE`.

**Context:** `src/load/__init__.py`'s original placeholder docstring
described "full refresh (truncate + insert)" as the MVP loading
strategy. That description predates ADR-008 (daily snapshot by DAG run
date, accumulating forward, never rewriting other days). A whole-table
truncate on every run would delete all previously accumulated snapshot
history and leave only the current run's day — directly undoing the
reason a snapshot model was chosen over a created_at cohort in the
first place. Per-day partition drop+reload gives the same idempotency
guarantee (safe to retry/backfill a given day) without touching other
days.

**Alternatives considered:**
- Whole-table `TRUNATE` + insert (the original placeholder plan) —
  simplest, but incompatible with ADR-008 as described above.
- `ALTER TABLE ... DELETE WHERE snapshot_date = ...` instead of
  `DROP PARTITION` — works without partitioning, but ClickHouse
  mutations are asynchronous background operations, not immediate;
  `DROP PARTITION` on an explicitly date-partitioned table is a
  lightweight, synchronous metadata operation and the standard
  ClickHouse pattern for exactly this "reload one day" scenario.
- Monthly partitions (`PARTITION BY toYYYYMM(snapshot_date)`) — fewer
  partitions overall, but `DROP PARTITION` would then delete an entire
  month's data to reload one day within it; daily partitions are the
  only granularity that matches the reload boundary we actually need.

**Consequences:**
- `src/load/__init__.py`'s docstring updated to describe per-day
  partition refresh instead of whole-table truncate.
- Not atomic across the two statements (drop, then insert) — ClickHouse
  has no cross-statement transactions. A failure between them leaves
  that day's partition empty until the next successful run; acceptable
  given Airflow's own retry mechanism is the natural recovery path for
  an idempotent per-day task.
- `client` is passed into `load_daily_task_snapshot()` and `ensure_table()`
  explicitly, not created internally — same DI convention as petrag's
  `ingestion/vector_store.py`, keeps both functions testable with a fake
  client.

---

## Template for new ADRs

```
## ADR-00N — Title

**Date:**
**Status:** Accepted / Superseded by ADR-00X / Deprecated

**Decision:**

**Context:**

**Alternatives considered:**

**Consequences:**
```