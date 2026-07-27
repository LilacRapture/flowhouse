"""
Dash entrypoint for the flowhouse dashboard.

Wraps the whole app behind HTTP Basic Auth via a plain Flask
before_request hook — no dash-auth dependency. /health is the one
deliberate exception (see _PUBLIC_PATHS) — needed for Docker's own
healthcheck and Compose's `--wait`/`depends_on: condition: service_healthy`,
neither of which can supply credentials.

app.layout is assigned a FUNCTION, not a fixed Dash component tree —
Dash calls that function fresh on every page load/session, which is
how ClickHouse gets queried per visit without a dcc.Interval poll.
"""
import hmac
import os

import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html
from flask import Response, jsonify, request

from queries import (
    get_client,
    get_distinct_projects,
    get_overdue_trend,
    get_task_count_by_project_status,
)

DASHBOARD_USER = os.environ.get("DASHBOARD_USER", "admin")
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "")

_ALL_PROJECTS_VALUE = "__all__"
_ALL_PROJECTS_LABEL = "All projects"

# Paths that bypass Basic Auth entirely. Deliberately a small, explicit
# allowlist rather than a pattern/prefix match — every entry here is a
# conscious decision to expose that specific path, not an accident of a
# broad regex.
_PUBLIC_PATHS = {"/health"}

app = Dash(
    __name__,
    # No @app.callback in this app — layout is queried fresh per
    # request via serve_layout() instead.
    # Without this flag, Dash eagerly calls serve_layout() once at
    # `app.layout = serve_layout` assignment time to build an internal
    # validation_layout for callback-ID checks — which means importing
    # this module at all (e.g. in tests, or just `python -c "import
    # app"`) triggers a real ClickHouse connection attempt.
    suppress_callback_exceptions=True,
)
server = app.server


def _check_auth(username: str, password: str) -> bool:
    """
    hmac.compare_digest instead of `==` — constant-time comparison,
    avoids leaking password length/prefix via response-timing
    differences.
    """
    return hmac.compare_digest(username, DASHBOARD_USER) and hmac.compare_digest(
        password, DASHBOARD_PASSWORD
    )


def _authentication_required() -> Response:
    return Response(
        "Authentication required.",
        401,
        {"WWW-Authenticate": 'Basic realm="flowhouse dashboard"'},
    )


@server.before_request
def _require_basic_auth():
    """
    Runs before EVERY request Flask receives — including ones that don't
    match any Dash route — since Flask's before_request hooks execute
    ahead of URL-routing/404 resolution. Returning a Response here short-
    circuits the request; returning None lets it proceed normally.
    """
    if request.path in _PUBLIC_PATHS:
        return None

    auth = request.authorization
    if not auth or not _check_auth(auth.username, auth.password):
        return _authentication_required()


@server.route("/health")
def _health():
    """
    Liveness probe for Docker/Compose healthchecks — deliberately does
    NOT touch ClickHouse. This answers "is the Flask/Dash process up and
    routing requests", not "can it reach its data source". A DB-down
    scenario should surface as a broken chart on the actual dashboard
    page, not a failed container healthcheck that restarts a perfectly
    healthy process.
    """
    return jsonify(status="ok"), 200


def _build_overdue_trend_figure() -> go.Figure:
    client = get_client()
    rows = get_overdue_trend(client)

    dates = [row[0] for row in rows]
    counts = [row[1] for row in rows]

    figure = go.Figure(data=[go.Scatter(x=dates, y=counts, mode="lines+markers")])
    figure.update_layout(
        title="Overdue tasks over time",
        xaxis_title="Date",
        yaxis_title="Overdue task count",
    )
    return figure


def _figure_from_task_count_rows(rows: list[tuple]) -> go.Figure:
    """
    Builds the grouped bar chart from already-fetched-and-filtered rows.
    Split out from _build_task_count_figure() so the project-filter
    callback can reuse the plotting logic without re-running SQL, and
    so it's unit-testable with plain Python tuples — no ClickHouse
    client involved at all.
    """
    projects = sorted({row[0] for row in rows})
    statuses = sorted({row[1] for row in rows})
    counts_by_project_status = {(row[0], row[1]): row[2] for row in rows}

    traces = [
        go.Bar(
            name=status,
            x=projects,
            y=[counts_by_project_status.get((project, status), 0) for project in projects],
        )
        for status in statuses
    ]

    figure = go.Figure(data=traces)
    figure.update_layout(
        title="Task count by project and status",
        xaxis_title="Project",
        yaxis_title="Task count",
        barmode="group",
    )
    return figure


def _build_task_count_figure(selected_project: str = _ALL_PROJECTS_VALUE) -> go.Figure:
    """
    Fetches current task-count rows and, if a specific project (not the
    "All projects" sentinel) is selected, narrows to just that project
    before building the figure. Filtering happens in Python, not SQL —
    get_task_count_by_project_status()'s result set is small (one row
    per project/status pair), so a second round-trip per dropdown change
    isn't worth the extra query variant.
    """
    client = get_client()
    rows = get_task_count_by_project_status(client)

    if selected_project and selected_project != _ALL_PROJECTS_VALUE:
        rows = [row for row in rows if row[0] == selected_project]

    return _figure_from_task_count_rows(rows)


def _project_dropdown_options() -> list[dict]:
    client = get_client()
    projects = get_distinct_projects(client)

    return [{"label": _ALL_PROJECTS_LABEL, "value": _ALL_PROJECTS_VALUE}] + [
        {"label": project, "value": project} for project in projects
    ]


def serve_layout() -> html.Div:
    """
    Assigned to app.layout as a callable (see module docstring) so it
    re-runs — and re-queries ClickHouse — on every page load, instead of
    being computed once at process startup.
    """
    return html.Div(
        [
            html.H1("flowhouse — TaskTracker analytics"),
            dcc.Graph(id="overdue-trend", figure=_build_overdue_trend_figure()),
            html.Label("Filter by project:", htmlFor="project-filter"),
            dcc.Dropdown(
                id="project-filter",
                options=_project_dropdown_options(),
                value=_ALL_PROJECTS_VALUE,
                clearable=False,
            ),
            dcc.Graph(
                id="task-count-by-project-status",
                figure=_build_task_count_figure(),
            ),
        ]
    )


@app.callback(
    Output("task-count-by-project-status", "figure"),
    Input("project-filter", "value"),
)
def _update_task_count_figure(selected_project: str) -> go.Figure:
    return _build_task_count_figure(selected_project)


app.layout = serve_layout


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
