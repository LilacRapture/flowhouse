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
  on the way in, regardless of how the file was written. The transform
  step (Phase 1, not written yet) must call
  `pd.read_parquet(path, dtype_backend="numpy_nullable")` to actually get
  proper nullable `Int64` columns — noted in `src/transform/__init__.py`
  so this isn't rediscovered the hard way later.

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