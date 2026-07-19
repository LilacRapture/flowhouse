"""
Extractor for TaskTracker's REST API.

Authenticates via TaskTracker's own JWT login endpoint using credentials
stored in the `tasktracker_api` Airflow Connection (env-var format, see
docs/decisions.md ADR-005). The Connection is used only as a generic
credential container here — TaskTracker's /api/auth/login/ expects a
JSON body, not HTTP Basic Auth, so `conn.login` / `conn.password` are
passed as the email/password pair rather than used as an auth header.

Paginates through /api/tasks/ and /api/projects/ (DRF PageNumberPagination
— follows the `next` link until exhausted) and writes each resource to
its own parquet file under a shared volume. Only the resulting file path
is meant to cross an Airflow XCom boundary — never the DataFrame itself.
"""
import logging
import os
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)

CONN_ID = "tasktracker_api"
DATA_DIR = os.environ.get("FLOWHOUSE_DATA_DIR", "/opt/airflow/data/raw")
_REQUEST_TIMEOUT = 10  # seconds


def _base_url(conn) -> str:
    scheme = conn.conn_type or "http"
    port = f":{conn.port}" if conn.port else ""
    return f"{scheme}://{conn.host}{port}"


def _login(session: requests.Session, base_url: str, email: str, password: str) -> str:
    """Logs into TaskTracker and returns an access token."""
    response = session.post(
        f"{base_url}/api/auth/login/",
        json={"email": email, "password": password},
        timeout=_REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()["access"]


def _fetch_all_pages(session: requests.Session, url: str) -> list[dict]:
    """
    Follows DRF's PageNumberPagination `next` links until exhausted.
    Same response shape for both /api/tasks/ and /api/projects/.
    """
    records: list[dict] = []
    while url:
        response = session.get(url, timeout=_REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        records.extend(payload["results"])
        url = payload.get("next")
    return records


def _write_parquet(records: list[dict], resource: str) -> str:
    """
    Writes raw records straight to parquet via pyarrow, without building a
    pandas DataFrame first. Two reasons, not just one: (1) extract's job
    is to persist data unmodified — no pandas-specific processing happens
    here, so there's no need for its API; (2) pandas silently upcasts
    integer columns containing None to float64 (e.g. a nullable FK id
    becomes 5.0 instead of 5) — pyarrow keeps them as nullable int64. The
    transform step (pandas, by design) reads this file back afterwards.
    """
    os.makedirs(DATA_DIR, exist_ok=True)
    run_date = datetime.now(UTC).strftime("%Y-%m-%d")
    path = os.path.join(DATA_DIR, f"{resource}_{run_date}.parquet")

    table = pa.Table.from_pylist(records)
    pq.write_table(table, path)

    logger.info("Wrote %d %s record(s) to %s", len(records), resource, path)
    return path


def _extract_resource(resource: str) -> str:
    """
    Shared by extract_tasks() / extract_projects(): logs in once, pages
    through GET /api/{resource}/, writes parquet, and returns its path.
    """
    conn = BaseHook.get_connection(CONN_ID)
    base_url = _base_url(conn)

    with requests.Session() as session:
        token = _login(session, base_url, conn.login, conn.password)
        session.headers["Authorization"] = f"Bearer {token}"
        records = _fetch_all_pages(session, f"{base_url}/api/{resource}/")

    return _write_parquet(records, resource)


def extract_tasks() -> str:
    """Airflow task entrypoint. Returns the parquet path (XCom-safe)."""
    return _extract_resource("tasks")


def extract_projects() -> str:
    """Airflow task entrypoint. Returns the parquet path (XCom-safe)."""
    return _extract_resource("projects")
    