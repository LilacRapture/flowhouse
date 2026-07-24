"""
Tests the pure logic (login, pagination, parquet write) with mocked HTTP —
no real TaskTracker instance needed. `_extract_resource` itself (which
calls `BaseHook.get_connection`) needs a live Airflow context, so it's
not covered here; that's exercised by actually running the DAG (see
README.md).
"""
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq

from src.extract.tasktracker import (
    _TASKS_ARROW_SCHEMA,
    _base_url,
    _fetch_all_pages,
    _login,
    _write_parquet,
    extract_projects,
    extract_tasks,
)


class FakeConn:
    conn_type = "http"
    host = "host.docker.internal"
    port = 8000


def test_base_url_includes_port():
    assert _base_url(FakeConn()) == "http://host.docker.internal:8000"


def test_login_posts_credentials_and_returns_access_token():
    session = MagicMock()
    session.post.return_value.json.return_value = {"access": "fake-token", "refresh": "x"}

    token = _login(session, "http://host:8000", "admin@example.com", "secret")

    assert token == "fake-token"
    session.post.assert_called_once_with(
        "http://host:8000/api/auth/login/",
        json={"email": "admin@example.com", "password": "secret"},
        timeout=10,
    )


def test_fetch_all_pages_follows_next_link():
    session = MagicMock()
    page_1 = MagicMock()
    page_1.json.return_value = {"results": [{"id": 1}], "next": "http://host/api/tasks/?page=2"}
    page_2 = MagicMock()
    page_2.json.return_value = {"results": [{"id": 2}], "next": None}
    session.get.side_effect = [page_1, page_2]

    records = _fetch_all_pages(session, "http://host/api/tasks/")

    assert records == [{"id": 1}, {"id": 2}]
    assert session.get.call_count == 2


def test_fetch_all_pages_empty_result():
    session = MagicMock()
    session.get.return_value.json.return_value = {"results": [], "next": None}

    assert _fetch_all_pages(session, "http://host/api/tasks/") == []


def test_write_parquet_roundtrips(tmp_path, monkeypatch):
    monkeypatch.setattr("src.extract.tasktracker.DATA_DIR", str(tmp_path))

    path = _write_parquet([{"id": 1, "title": "Test task"}], "tasks")

    assert path.startswith(str(tmp_path))
    table = pq.read_table(path)
    assert table.to_pylist() == [{"id": 1, "title": "Test task"}]


def test_write_parquet_empty_records_still_writes_file(tmp_path, monkeypatch):
    monkeypatch.setattr("src.extract.tasktracker.DATA_DIR", str(tmp_path))

    path = _write_parquet([], "projects")
    table = pq.read_table(path)

    assert table.num_rows == 0


def test_write_parquet_keeps_nullable_int_as_int_not_float(tmp_path, monkeypatch):
    """
    Regression test for the pandas footgun that motivated writing via
    pyarrow directly (ADR-006): a nullable int column must stay int64
    with a proper null IN THE FILE ITSELF.

    Checked via pyarrow's own reader, not pd.read_parquet() — pandas'
    default read_parquet() still upcasts int64+null back to float64 on
    the way IN, regardless of how the file was written. The transform
    step must read with dtype_backend="numpy_nullable" to actually get
    the benefit (see src/transform/__init__.py).
    """
    monkeypatch.setattr("src.extract.tasktracker.DATA_DIR", str(tmp_path))
    records = [{"id": 1, "project_id": 5}, {"id": 2, "project_id": None}]

    path = _write_parquet(records, "tasks")
    table = pq.read_table(path)

    assert table.schema.field("project_id").type == pa.int64()
    assert table.column("project_id").to_pylist() == [5, None]
    
# ---------------------------------------------------------------------------
# _write_parquet — explicit schema for tasks (ADR-018 regression tests)
# ---------------------------------------------------------------------------

def _all_none_project_records() -> list[dict]:
    """
    Two tasks where EVERY record has project=None — the exact shape
    that triggers pyarrow's type-inference gap (ADR-018). A single
    record wouldn't be enough to prove the point convincingly on its
    own, but the bug already reproduces with just one; two is here
    mainly for readability of "a batch", not a strict requirement.
    """
    base = {
        "title": "Task", "description": "", "status": "todo", "due_date": None,
        "owner": {"id": 1, "email": "a@example.com", "full_name": "A"},
        "project": None,
        "created_at": "2026-06-01T10:00:00Z", "updated_at": "2026-06-01T10:00:00Z",
    }
    return [{"id": 1, **base}, {"id": 2, **base}]


def test_write_parquet_without_schema_infers_null_type_for_all_none_project(tmp_path, monkeypatch):
    """
    Documents WHY _TASKS_ARROW_SCHEMA exists: confirms the failure mode
    is real pyarrow behavior, not a hypothetical — without an explicit
    schema, a column where every value is None gets inferred as
    pyarrow's `null` type, not struct<id, name>. This is what made
    Spark's `F.col("project.id")` fail with "Can't extract a value from
    project" (see tests/test_spark_ops.py, ADR-018).
    """
    monkeypatch.setattr("src.extract.tasktracker.DATA_DIR", str(tmp_path))

    path = _write_parquet(_all_none_project_records(), "tasks")  # no schema
    table = pq.read_table(path)

    assert pa.types.is_null(table.schema.field("project").type)


def test_write_parquet_with_explicit_schema_keeps_project_as_struct(tmp_path, monkeypatch):
    """
    The actual fix: passing _TASKS_ARROW_SCHEMA keeps `project` typed as
    struct<id, name> even when every record's project is None.
    """
    monkeypatch.setattr("src.extract.tasktracker.DATA_DIR", str(tmp_path))

    path = _write_parquet(_all_none_project_records(), "tasks", schema=_TASKS_ARROW_SCHEMA)
    table = pq.read_table(path)

    project_field = table.schema.field("project")
    assert pa.types.is_struct(project_field.type)
    assert [f.name for f in project_field.type] == ["id", "name"]


def test_extract_tasks_always_passes_explicit_schema(monkeypatch):
    """
    Regression test for the actual production wiring, not just
    _write_parquet's capability: extract_tasks() must itself pass
    _TASKS_ARROW_SCHEMA to _extract_resource(). This is what would
    catch someone accidentally dropping the schema= argument from
    extract_tasks() in the future — test_write_parquet_with_explicit_
    schema_keeps_project_as_struct above would keep passing even if
    that happened, since it calls _write_parquet directly.
    """
    captured = {}

    def _fake_extract_resource(resource, schema=None):
        captured["resource"] = resource
        captured["schema"] = schema
        return "fake_path.parquet"

    monkeypatch.setattr("src.extract.tasktracker._extract_resource", _fake_extract_resource)

    extract_tasks()

    assert captured["resource"] == "tasks"
    assert captured["schema"] is _TASKS_ARROW_SCHEMA


def test_extract_projects_does_not_pass_a_schema(monkeypatch):
    """
    extract_projects() is intentionally unaffected by ADR-018 — its
    records are flat with no nested struct field that could collapse
    to pyarrow's null type. Confirms the fix wasn't blanket-applied.
    """
    captured = {}

    def _fake_extract_resource(resource, schema=None):
        captured["schema"] = schema
        return "fake_path.parquet"

    monkeypatch.setattr("src.extract.tasktracker._extract_resource", _fake_extract_resource)

    extract_projects()

    assert captured["schema"] is None
