"""
Tests for the project-filter dropdown/callback added on top of the
existing task-count bar chart. Pure-logic tests — no browser, no real
Dash callback dispatch. A Playwright E2E test is what verifies 
clicking the actual dropdown in a real browser triggers this
same code path end-to-end.

Overrides the session-wide `_no_real_clickhouse_connections` autouse
fixture's fake client (which always returns empty rows, see conftest.py)
with one that returns real rows, scoped to each test that needs it.
"""
import app


class _FakeClickHouseClientWithData:
    """Returns canned rows regardless of the SQL text — same minimal
    shape as conftest.py's fake, but with non-empty result_rows."""

    def __init__(self, result_rows):
        self._result_rows = result_rows

    def query(self, sql, parameters=None):
        class _Result:
            result_rows = self._result_rows

        return _Result()


_SAMPLE_TASK_COUNT_ROWS = [
    ("Website Redesign", "todo", 3),
    ("Website Redesign", "done", 1),
    ("(no project)", "todo", 2),
]


# ---------------------------------------------------------------------------
# _figure_from_task_count_rows — pure function, no client at all
# ---------------------------------------------------------------------------

def test_figure_from_task_count_rows_builds_one_trace_per_status():
    figure = app._figure_from_task_count_rows(_SAMPLE_TASK_COUNT_ROWS)
    trace_names = {trace.name for trace in figure.data}
    assert trace_names == {"todo", "done"}


def test_figure_from_task_count_rows_empty_rows_still_returns_figure():
    figure = app._figure_from_task_count_rows([])
    assert figure.data == ()


# ---------------------------------------------------------------------------
# _build_task_count_figure — "All projects" vs a specific project
# ---------------------------------------------------------------------------

def test_build_task_count_figure_all_projects_includes_every_project(monkeypatch):
    monkeypatch.setattr(
        app, "get_client", lambda: _FakeClickHouseClientWithData(_SAMPLE_TASK_COUNT_ROWS)
    )

    figure = app._build_task_count_figure(app._ALL_PROJECTS_VALUE)

    all_x_values = {x for trace in figure.data for x in trace.x}
    assert all_x_values == {"Website Redesign", "(no project)"}


def test_build_task_count_figure_filters_to_selected_project(monkeypatch):
    monkeypatch.setattr(
        app, "get_client", lambda: _FakeClickHouseClientWithData(_SAMPLE_TASK_COUNT_ROWS)
    )

    figure = app._build_task_count_figure("Website Redesign")

    all_x_values = {x for trace in figure.data for x in trace.x}
    assert all_x_values == {"Website Redesign"}


def test_build_task_count_figure_unknown_project_returns_empty_figure(monkeypatch):
    """Defensive: a stale/removed project id in the URL or a race with
    data changing shouldn't crash the callback, just render nothing."""
    monkeypatch.setattr(
        app, "get_client", lambda: _FakeClickHouseClientWithData(_SAMPLE_TASK_COUNT_ROWS)
    )

    figure = app._build_task_count_figure("Nonexistent Project")

    all_x_values = {x for trace in figure.data for x in trace.x}
    assert all_x_values == set()


# ---------------------------------------------------------------------------
# _project_dropdown_options
# ---------------------------------------------------------------------------

def test_project_dropdown_options_includes_all_projects_sentinel_first(monkeypatch):
    monkeypatch.setattr(
        app,
        "get_client",
        lambda: _FakeClickHouseClientWithData(
            [("Website Redesign",), ("(no project)",)]
        ),
    )

    options = app._project_dropdown_options()

    assert options[0] == {
        "label": app._ALL_PROJECTS_LABEL,
        "value": app._ALL_PROJECTS_VALUE,
    }
    values = {opt["value"] for opt in options}
    assert values == {app._ALL_PROJECTS_VALUE, "Website Redesign", "(no project)"}


def test_project_dropdown_options_no_projects_still_has_all_sentinel(monkeypatch):
    monkeypatch.setattr(app, "get_client", lambda: _FakeClickHouseClientWithData([]))

    options = app._project_dropdown_options()

    assert options == [{"label": app._ALL_PROJECTS_LABEL, "value": app._ALL_PROJECTS_VALUE}]
