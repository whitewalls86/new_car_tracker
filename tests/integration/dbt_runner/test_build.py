"""
Integration tests for dbt_runner /dbt/build.

Build success tests call the dbt subprocess directly (same pattern as Layer 2) and
assert on the process exit code — no HTTP involved since the real complexity is whether
dbt itself succeeds for each intent's selector.

Validation error tests use TestClient — these fail before the subprocess is ever called,
so no real dbt invocation is needed.
"""
import pytest

from dbt_runner.app import _INTENT_FALLBACK

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Real dbt builds — one test per intent
# ---------------------------------------------------------------------------

class TestIntentBuilds:
    """Each intent's selector must produce a successful dbt build."""

    def test_after_srp_builds(self, run_dbt_intent):
        select = " ".join(_INTENT_FALLBACK["after_srp"])
        run_dbt_intent(select)

    def test_after_detail_builds(self, run_dbt_intent):
        select = " ".join(_INTENT_FALLBACK["after_detail"])
        run_dbt_intent(select)

    def test_both_builds(self, run_dbt_intent):
        select = " ".join(_INTENT_FALLBACK["both"])
        run_dbt_intent(select)


# ---------------------------------------------------------------------------
# Validation errors — TestClient, no subprocess invoked
# ---------------------------------------------------------------------------

class TestBuildValidation:

    def test_unknown_intent_returns_400(self, api_client):
        resp = api_client.post("/dbt/build", json={"intent": "nonexistent"})
        assert resp.status_code == 400
        assert "Unknown intent" in resp.json()["detail"]

    def test_no_intent_or_select_returns_400(self, api_client):
        resp = api_client.post("/dbt/build", json={})
        assert resp.status_code == 400
        is_response_intent = "intent" in resp.json()["detail"].lower()
        is_select_intent = "select" in resp.json()["detail"].lower()
        assert is_response_intent or is_select_intent

    def test_invalid_select_token_returns_400(self, api_client):
        resp = api_client.post("/dbt/build", json={"select": ["model name with spaces"]})
        assert resp.status_code == 400

    def test_invalid_exclude_token_returns_400(self, api_client):
        resp = api_client.post("/dbt/build", json={
            "select": ["stg_raw_artifacts+"],
            "exclude": ["model#tag"],
        })
        assert resp.status_code == 400
