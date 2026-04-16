"""
Layer 3 — deploy_intent state machine integration tests.

Covers GET /deploy/status, POST /deploy/start, POST /deploy/complete.

The deploy_intent table has exactly one row (id=1).  An autouse function-scoped
fixture resets it to intent='none' before and after every test, giving each test
a clean slate without relying on ordering.
"""
import uuid

import pytest


@pytest.fixture(autouse=True)
def reset_deploy_intent(verify_cur):
    """Reset deploy_intent to 'none' before and after every test in this module."""
    verify_cur.execute(
        "UPDATE deploy_intent SET intent='none', requested_at=NULL, requested_by=NULL WHERE id=1"
    )
    yield
    verify_cur.execute(
        "UPDATE deploy_intent SET intent='none', requested_at=NULL, requested_by=NULL WHERE id=1"
    )


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_deploy_status_returns_current_state(api_client):
    response = api_client.get("/deploy/status")

    assert response.status_code == 200
    data = response.json()
    for key in ("intent", "requested_at", "requested_by", "number_running", "min_started_at"):
        assert key in data, f"Missing key in /deploy/status response: {key}"


# ---------------------------------------------------------------------------
# Start
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_deploy_start_sets_intent(api_client, verify_cur):
    response = api_client.post("/deploy/start")

    assert response.status_code == 200
    assert response.json() is True

    verify_cur.execute(
        "SELECT intent, requested_by FROM deploy_intent WHERE id=1"
    )
    row = verify_cur.fetchone()
    assert row["intent"] == "pending"
    assert row["requested_by"] == "Deploy Declared"


@pytest.mark.integration
def test_deploy_start_idempotent_when_already_pending(api_client):
    api_client.post("/deploy/start")

    response = api_client.post("/deploy/start")

    assert response.status_code == 503


# ---------------------------------------------------------------------------
# Complete
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_deploy_complete_releases_intent(api_client, verify_cur):
    api_client.post("/deploy/start")

    response = api_client.post("/deploy/complete")

    assert response.status_code == 200
    assert response.json() is True

    verify_cur.execute(
        "SELECT intent, requested_at FROM deploy_intent WHERE id=1"
    )
    row = verify_cur.fetchone()
    assert row["intent"] == "none"
    assert row["requested_at"] is None


# ---------------------------------------------------------------------------
# Running count
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_deploy_status_reflects_running_count(api_client, verify_cur):
    run_id = str(uuid.uuid4())
    verify_cur.execute(
        "INSERT INTO runs (run_id, started_at, status, trigger)"
        " VALUES (%s, now(), 'running', 'l3test-deploy-count')",
        (run_id,),
    )
    try:
        response = api_client.get("/deploy/status")
        assert response.status_code == 200
        assert response.json()["number_running"] >= 1
    finally:
        verify_cur.execute("DELETE FROM runs WHERE run_id = %s", (run_id,))
