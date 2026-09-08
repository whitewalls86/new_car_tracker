"""
Layer 4 — deploy_intent state machine integration tests.

Covers GET /deploy/status, POST /deploy/start, POST /deploy/complete.

Both compatibility tables have exactly one row (id=1). An autouse
function-scoped fixture resets them before and after every test, giving each
test a clean slate without relying on ordering.
"""
import uuid

import pytest

from ops.coordination_contract import SERVICE_CONTRACTS, expand_targets
from tests.sql_loader import queries

SQL = queries(__file__)


@pytest.fixture(autouse=True)
def reset_deploy_intent(verify_cur):
    """Reset both sides of the dual-signal compatibility contract."""
    verify_cur.execute(
        SQL("update_deploy_intent_intent")
    )
    verify_cur.execute(SQL("reset_coordination_state"))
    yield
    verify_cur.execute(
        SQL("update_deploy_intent_intent")
    )
    verify_cur.execute(SQL("reset_coordination_state"))


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
        SQL("select_intent_requested_by_from_deploy_intent")
    )
    row = verify_cur.fetchone()
    assert row["intent"] == "pending"
    assert row["requested_by"] == "Deploy Declared"

    verify_cur.execute(
        SQL("select_kind_phase_generation_from_coordination_state")
    )
    coordination = verify_cur.fetchone()
    assert coordination["kind"] == "deploy"
    assert coordination["phase"] == "requested"
    assert coordination["generation"] >= 1
    assert set(coordination["targets"])
    assert set(coordination["scope"]) == {
        "airflow_control",
        "analytics",
        "archive",
        "database",
        "detail_fetch",
        "ingress",
        "listing_fetch",
        "observability",
        "processing",
    }


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
        SQL("select_intent_requested_at_from_deploy_intent")
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


@pytest.mark.integration
def test_a_release_refused_by_another_coordination_kind_says_who_holds_it(
    api_client, verify_cur
):
    """The refusal reads off the real row, not a fabricated tuple.

    `_intent_release` indexes `SELECT_COORDINATION_STATE_FOR_DEPLOY`'s columns
    by position to name the holder. A unit test builds that tuple itself, so it
    would pass just as happily if the statement's column order changed under it
    and the message started naming a phase as a kind.

    Until Plan 162 Stage K this answered 503 "Database unavailable" -- for a
    facade correctly declining to release a host window. Plan 171 owns the rest
    of that vocabulary.
    """
    verify_cur.execute(
        SQL("update_coordination_state_kind")
    )

    response = api_client.post("/deploy/complete")

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert "host_maintenance" in detail
    assert "active" in detail
    assert "Database unavailable" not in detail


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

    verify_cur.execute(SQL("select_pause_long_jobs_from_deploy_intent"))
    assert verify_cur.fetchone()["pause_long_jobs"] is True


@pytest.mark.integration
def test_deploy_start_can_opt_out_of_pausing(api_client, verify_cur):
    api_client.post("/deploy/start", json={"pause_long_jobs": False})

    verify_cur.execute(SQL("select_pause_long_jobs_from_deploy_intent"))
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
        SQL("insert_detail_scrape_claims"),
        (listing_id, str(uuid.uuid4())),
    )
    try:
        response = api_client.get("/deploy/status")
        assert response.status_code == 200
        assert response.json()["number_running"] >= 1
    finally:
        verify_cur.execute(SQL("delete_detail_scrape_claims"), (listing_id,))


# ---------------------------------------------------------------------------
# Every service contract produces a row the database accepts (Plan 162 Stage K)
#
# `ops/coordination_contract.py` decides the (targets, scope) pair; the CHECK
# constraint added by V043 decides whether that pair can be stored. Both were
# authored by Plan 142 and nothing composed them, so `dashboard` and `pgadmin`
# -- the two services that map to no surfaces, deliberately and with a recorded
# reason -- could never be deployed alone. It surfaced on a production deploy on
# 2026-09-01 as 503 "Database unavailable" against a Postgres that was healthy
# throughout.
#
# This has to run against a real, migrated database. A Python restatement of the
# constraint would be a second source to keep in step with the first, which is
# the shape of the defect rather than a test for it.
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.parametrize("service", sorted(SERVICE_CONTRACTS))
def test_every_service_contract_yields_an_intent_row_the_database_accepts(
    service, api_client, verify_cur
):
    """One lone deploy per registered service, written for real.

    Parametrised over the contract itself, so a service added with a pair the
    constraint refuses fails here under its own name rather than on the deploy
    that first needs it.
    """
    response = api_client.post("/deploy/start", json={"targets": [service]})

    assert response.status_code == 200, (
        f"a lone deploy of {service} was refused: {response.json()}"
    )

    verify_cur.execute(
        SQL("select_kind_phase_targets_from_coordination_state")
    )
    row = verify_cur.fetchone()
    expected_targets, expected_scope = expand_targets({service})
    assert row["kind"] == "deploy"
    assert row["phase"] == "requested"
    assert set(row["targets"]) == set(expected_targets)
    assert set(row["scope"]) == set(expected_scope)


@pytest.mark.integration
@pytest.mark.parametrize(
    "service", sorted(s for s, c in SERVICE_CONTRACTS.items() if not c.surfaces)
)
def test_a_deploy_that_pauses_no_surface_is_stored_and_drains(
    service, api_client, verify_cur
):
    """Storing the row is half the claim; the readers are the other half.

    V050 stopped requiring a non-empty scope on the argument that an empty one
    is truthful -- this coordination pauses nothing -- and that the drain agrees.
    This runs the request through to authorization to show it does, rather than
    leaving the argument in the migration's comment.
    """
    assert api_client.post(
        "/deploy/start", json={"targets": [service]}
    ).status_code == 200

    verify_cur.execute(SQL("select_scope_from_coordination_state"))
    assert verify_cur.fetchone()["scope"] == []

    assert api_client.post("/coordination/begin-drain").status_code == 200

    drain = api_client.get("/coordination/drain-status").json()
    assert drain["drained"] is True, drain
    assert drain["blockers"] == []

    assert api_client.post("/coordination/authorize").status_code == 200

    verify_cur.execute(SQL("select_phase_from_coordination_state"))
    assert verify_cur.fetchone()["phase"] == "active"
