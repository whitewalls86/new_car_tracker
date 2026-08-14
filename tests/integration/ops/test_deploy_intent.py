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
        "UPDATE deploy_intent SET intent='none', requested_at=NULL, "
        "requested_by=NULL, pause_long_jobs=true WHERE id=1"
    )
    yield
    verify_cur.execute(
        "UPDATE deploy_intent SET intent='none', requested_at=NULL, "
        "requested_by=NULL, pause_long_jobs=true WHERE id=1"
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

    assert response.status_code == 409


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


@pytest.mark.integration
def test_deploy_start_returns_409_not_503_when_locked(api_client):
    """Locked intent must return 409, not 503 — confirms error discrimination."""
    api_client.post("/deploy/start")

    response = api_client.post("/deploy/start")

    assert response.status_code == 409
    assert "already" in response.json()["detail"].lower()


@pytest.mark.integration
def test_deploy_complete_when_no_intent_set(api_client):
    """Complete with no intent set still returns 200 — release is unconditional."""
    response = api_client.post("/deploy/complete")

    assert response.status_code == 200


# ---------------------------------------------------------------------------
# pause_long_jobs (Plan 131 Stage 5 D3b, migration V042)
#
# This is where the column, the grant and the semantics are checked against a
# real database rather than a mock. The archiver reads this table as
# scraper_user, which had never read it before Plan 131.
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_deploy_start_defaults_to_pausing_long_jobs(api_client, verify_cur):
    api_client.post("/deploy/start")

    verify_cur.execute("SELECT pause_long_jobs FROM deploy_intent WHERE id=1")
    assert verify_cur.fetchone()["pause_long_jobs"] is True


@pytest.mark.integration
def test_deploy_start_can_opt_out_of_pausing(api_client, verify_cur):
    api_client.post("/deploy/start", json={"pause_long_jobs": False})

    verify_cur.execute("SELECT pause_long_jobs FROM deploy_intent WHERE id=1")
    assert verify_cur.fetchone()["pause_long_jobs"] is False


@pytest.mark.integration
def test_deploy_status_reports_pause_long_jobs(api_client):
    api_client.post("/deploy/start", json={"pause_long_jobs": False})

    assert api_client.get("/deploy/status").json()["pause_long_jobs"] is False


@pytest.mark.integration
def test_long_jobs_paused_reads_the_real_table(api_client):
    """The helper both processors call, against the real schema."""
    from shared.deploy_intent import long_jobs_paused

    assert long_jobs_paused() is False, "no deploy pending"

    api_client.post("/deploy/start")
    assert long_jobs_paused() is True

    api_client.post("/deploy/complete")
    assert long_jobs_paused() is False, "a released intent stops pausing"


@pytest.mark.integration
def test_a_deploy_that_opted_out_does_not_pause_long_jobs(api_client):
    from shared.deploy_intent import long_jobs_paused

    api_client.post("/deploy/start", json={"pause_long_jobs": False})

    # Intent is pending, but this deploy touches nothing the jobs depend on.
    assert api_client.get("/deploy/status").json()["intent"] == "pending"
    assert long_jobs_paused() is False


@pytest.mark.integration
def test_scraper_user_can_read_deploy_intent(verify_cur):
    """The archiver connects as scraper_user; D3b said to confirm, not assume."""
    verify_cur.execute(
        "SELECT has_table_privilege('scraper_user', 'deploy_intent', 'SELECT') AS ok"
    )
    assert verify_cur.fetchone()["ok"] is True


# ---------------------------------------------------------------------------
# Running count
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_deploy_status_reflects_running_count(api_client, verify_cur):
    listing_id = str(uuid.uuid4())
    verify_cur.execute(
        "INSERT INTO detail_scrape_claims (listing_id, claimed_by, status)"
        " VALUES (%s, %s, 'running')",
        (listing_id, str(uuid.uuid4())),
    )
    try:
        response = api_client.get("/deploy/status")
        assert response.status_code == 200
        assert response.json()["number_running"] >= 1
    finally:
        verify_cur.execute("DELETE FROM detail_scrape_claims WHERE listing_id = %s", (listing_id,))
