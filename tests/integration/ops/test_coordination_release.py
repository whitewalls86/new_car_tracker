"""Real-database coverage for the Plan 142 Stage 3 release lifecycle."""

import hashlib

import pytest

from ops.routers import coordination
from scripts.host_maintenance import HOST_VALIDATION_GATES

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def reset_coordination_release(verify_cur):
    verify_cur.execute(
        "UPDATE coordination_state SET kind=NULL, phase='none', "
        "targets='[]'::jsonb, scope='[]'::jsonb WHERE id=1"
    )
    yield
    verify_cur.execute(
        "UPDATE coordination_state SET kind=NULL, phase='none', "
        "targets='[]'::jsonb, scope='[]'::jsonb WHERE id=1"
    )


def _host_evidence(generation):
    return {
        "generation": generation,
        "gates": {
            gate: {"verdict": "pass", "reason": "integration fixture"}
            for gate in HOST_VALIDATION_GATES
        },
        "evidence_digests": {"preflight": "a" * 64, "manifest": "b" * 64},
    }


def test_complete_is_replay_confirmable_against_real_database(
    api_client, verify_cur, mocker
):
    """A timed-out client can repeat complete without reopening authorization."""
    verify_cur.execute(
        """UPDATE coordination_state
              SET kind='host_maintenance', phase='validating', generation=generation + 1,
                  targets='["host"]'::jsonb, scope='["host"]'::jsonb,
                  requested_by='integration', manifest_location='/integration/manifest'
            WHERE id=1
        RETURNING generation"""
    )
    generation = verify_cur.fetchone()["generation"]
    manifest_sha256 = hashlib.sha256(b"integration manifest").hexdigest()
    mocker.patch.object(
        coordination,
        "collect_release_status",
        lambda _: {"blockers": [], "gates": []},
    )

    evidence = api_client.post("/coordination/host-evidence", json=_host_evidence(generation))
    assert evidence.status_code == 200
    payload = {
        "confirm_complete": True,
        "generation": generation,
        "manifest_sha256": manifest_sha256,
    }
    assert api_client.post("/coordination/complete", json=payload).json() == {
        "phase": "none",
        "generation": generation,
    }
    assert api_client.post("/coordination/complete", json=payload).json() == {
        "phase": "none",
        "generation": generation,
    }

    verify_cur.execute("SELECT phase, kind FROM coordination_state WHERE id=1")
    assert verify_cur.fetchone() == {"phase": "none", "kind": None}
    verify_cur.execute(
        "SELECT count(*) AS count FROM staging.coordination_state_events "
        "WHERE generation=%s AND prior_phase='validating' AND phase='none'",
        (generation,),
    )
    assert verify_cur.fetchone()["count"] == 1
    verify_cur.execute(
        "SELECT count(*) AS count FROM staging.coordination_release_evidence WHERE generation=%s",
        (generation,),
    )
    assert verify_cur.fetchone()["count"] == 1
