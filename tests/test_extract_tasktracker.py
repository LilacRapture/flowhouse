"""
Tests the pure logic (login, pagination, parquet write) with mocked HTTP —
no real TaskTracker instance needed. `_extract_resource` itself (which
calls `BaseHook.get_connection`) needs a live Airflow context, so it's
not covered here; that's exercised by actually running the DAG (see
README.md).
"""
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.extract.tasktracker import _base_url, _fetch_all_pages, _login, _write_parquet


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
    assert pd.read_parquet(path).to_dict("records") == [{"id": 1, "title": "Test task"}]


def test_write_parquet_empty_records_still_writes_file(tmp_path, monkeypatch):
    monkeypatch.setattr("src.extract.tasktracker.DATA_DIR", str(tmp_path))

    path = _write_parquet([], "projects")

    assert pd.read_parquet(path).empty
    