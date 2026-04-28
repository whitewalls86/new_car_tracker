"""
Integration tests for dbt_runner API.

Validation error tests use TestClient — these fail before any dbt subprocess
is invoked, so no real dbt installation is needed.
"""
import pytest

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Health / readiness endpoints
# ---------------------------------------------------------------------------

class TestHealthEndpoints:
    def test_health_returns_ok(self, api_client):
        resp = api_client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    def test_ready_returns_status(self, api_client):
        resp = api_client.get("/ready")
        assert resp.status_code == 200
        assert "ready" in resp.json()


# ---------------------------------------------------------------------------
# Build validation — TestClient, no subprocess invoked
# ---------------------------------------------------------------------------

class TestBuildValidation:
    def test_invalid_select_token_returns_400(self, api_client):
        resp = api_client.post("/dbt/build", json={"select": ["model name with spaces"]})
        assert resp.status_code == 400

    def test_invalid_exclude_token_returns_400(self, api_client):
        resp = api_client.post("/dbt/build", json={
            "select": ["stg_observations"],
            "exclude": ["model#tag"],
        })
        assert resp.status_code == 400

    def test_empty_string_select_token_returns_400(self, api_client):
        resp = api_client.post("/dbt/build", json={"select": [""]})
        assert resp.status_code == 400

    def test_shell_injection_token_rejected(self, api_client):
        resp = api_client.post("/dbt/build", json={"select": ["model; rm -rf /"]})
        assert resp.status_code == 400
