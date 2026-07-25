"""
Tests for the Basic Auth boundary in app.py — deliberately does NOT
assert on Dash's own routing behavior (Dash 4.x serves its SPA shell
for any path, including nonexistent ones — see
test_request_with_correct_credentials_passes_auth_boundary). Only
before_request's own 401/pass-through behavior is checked here.
"""
import base64

import pytest

from app import DASHBOARD_PASSWORD, DASHBOARD_USER, server

PROBE_PATH = "/_auth_probe_nonexistent"


@pytest.fixture
def client():
    return server.test_client()


def _basic_auth_header(username: str, password: str) -> dict:
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


def test_request_without_credentials_returns_401(client):
    response = client.get(PROBE_PATH)
    assert response.status_code == 401


def test_request_without_credentials_includes_www_authenticate_header(client):
    response = client.get(PROBE_PATH)
    assert "WWW-Authenticate" in response.headers
    assert "Basic" in response.headers["WWW-Authenticate"]


def test_request_with_wrong_password_returns_401(client):
    headers = _basic_auth_header(DASHBOARD_USER, "wrong-password")
    response = client.get(PROBE_PATH, headers=headers)
    assert response.status_code == 401


def test_request_with_wrong_username_returns_401(client):
    headers = _basic_auth_header("wrong-user", DASHBOARD_PASSWORD)
    response = client.get(PROBE_PATH, headers=headers)
    assert response.status_code == 401


def test_request_with_correct_credentials_passes_auth_boundary(client):
    """
    Correct credentials must NOT be blocked by before_request. We don't
    assert a specific status code beyond "not 401" — Dash serves its
    SPA shell for any path (including PROBE_PATH, which matches no real
    Dash route), so 200 here reflects Dash's own catch-all routing, not
    a guarantee about this specific path. The only thing this test
    verifies is that the auth check itself didn't block the request.
    """
    headers = _basic_auth_header(DASHBOARD_USER, DASHBOARD_PASSWORD)
    response = client.get(PROBE_PATH, headers=headers)
    assert response.status_code != 401
    