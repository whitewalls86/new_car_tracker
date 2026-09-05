"""Real-database coverage for the Plan 142 Stage 3 release lifecycle."""

import hashlib

import pytest

from ops.routers import coordination
from scripts.host_maintenance import HOST_VALIDATION_GATES
from tests.sql_loader import queries

SQL = queries(__file__)

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def reset_coordination_release(verify_cur):
    verify_cur.execute(
        SQL("update_coordination_state_kind")
    )
    yield
    verify_cur.execute(
        SQL("update_coordination_state_kind")
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
        SQL("update_coordination_state_kind_2")
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

    verify_cur.execute(SQL("select_phase_kind_from_coordination_state"))
    assert verify_cur.fetchone() == {"phase": "none", "kind": None}
    verify_cur.execute(
        SQL("select_count_from_staging_coordination_state_events"),
        (generation,),
    )
    assert verify_cur.fetchone()["count"] == 1
    verify_cur.execute(
        SQL("select_count_from_staging_coordination_release_evidence"),
        (generation,),
    )
    assert verify_cur.fetchone()["count"] == 1
