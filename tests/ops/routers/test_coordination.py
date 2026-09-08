"""Plan 142 Stage 1 coordination-state core."""

import json
from datetime import datetime
from pathlib import Path

import pytest

from ops.queries import (
    INSERT_COORDINATION_RELEASE_EVIDENCE,
    INSERT_COORDINATION_STATE_EVENT,
)
from ops.routers import coordination


def test_status_serializes_timestamps(mock_cursor_context):
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = {
        "kind": "deploy",
        "phase": "requested",
        "requested_by": "operator",
        "reason": "processing deploy",
        "targets": ["processing"],
        "scope": ["processing"],
        "requested_at": datetime.fromisoformat("2026-08-25T03:00:00+00:00"),
        "draining_at": None,
        "active_at": None,
        "validating_at": None,
        "completed_at": None,
        "expected_work": [],
        "manifest_location": None,
        "operator_notes": None,
        "updated_at": datetime.fromisoformat("2026-08-25T03:00:01+00:00"),
    }

    result = coordination._status()

    assert result["kind"] == "deploy"
    assert result["requested_at"] == "2026-08-25T03:00:00+00:00"
    assert result["scope"] == ["processing"]


def test_local_drain_exposes_named_ops_job_evidence(mock_client, mocker):
    mocker.patch(
        "ops.routers.coordination.job_snapshot",
        return_value={
            "active_jobs": 2,
            "oldest_started_at": "2026-08-25T01:00:00+00:00",
        },
    )

    response = mock_client.get("/coordination/local-drain")

    assert response.status_code == 200
    assert response.json() == {
        "source": "ops_jobs",
        "known": True,
        "active_jobs": 2,
        "oldest_started_at": "2026-08-25T01:00:00+00:00",
    }


@pytest.mark.parametrize(
    ("operation", "source", "target", "timestamp"),
    [
        ("begin-drain", "requested", "draining", "draining_at"),
        ("authorize", "draining", "active", "active_at"),
        ("begin-validation", "active", "validating", "validating_at"),
    ],
)
def test_forward_transitions(mock_cursor_context, operation, source, target, timestamp):
    _, cursor = mock_cursor_context
    cursor.fetchone.side_effect = [
        (source, 7, "service_maintenance", "operator"),
        (7,),
    ]

    assert coordination._transition(operation) == "ok"

    sql, params = cursor.execute.call_args_list[-2].args
    assert f"{timestamp} = now()" in sql
    assert params == (target,)
    event_sql, event_params = cursor.execute.call_args_list[-1].args
    assert event_sql == INSERT_COORDINATION_STATE_EVENT
    assert event_params == (7, source, target, "service_maintenance", "operator")


def test_illegal_transition_does_not_write(mock_cursor_context, caplog):
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = (
        "requested",
        7,
        "service_maintenance",
        "operator",
    )

    assert coordination._transition("authorize") == "conflict"
    assert len(cursor.execute.call_args_list) == 2
    refusal = caplog.records[-1]
    assert refusal.levelname == "WARNING"
    assert refusal.generation == 7
    assert refusal.prior_phase == "requested"
    assert refusal.phase == "active"
    assert refusal.kind == "service_maintenance"


@pytest.mark.parametrize("source", ["requested", "draining"])
def test_cancel_is_legal_before_authorization(mock_cursor_context, source):
    _, cursor = mock_cursor_context
    cursor.fetchone.side_effect = [
        (source, 7, "service_maintenance", "operator"),
        (8,),
    ]

    assert coordination._cancel() == "ok"
    sql = cursor.execute.call_args_list[-2].args[0]
    assert "phase = 'none'" in sql
    assert "generation = generation + 1" in sql
    assert cursor.execute.call_args_list[-1].args[1] == (
        8,
        source,
        "none",
        "service_maintenance",
        "operator",
    )


@pytest.mark.parametrize("source", ["none", "active", "validating"])
def test_cancel_refuses_unsafe_states(mock_cursor_context, source):
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = (
        source,
        7,
        "service_maintenance",
        "operator",
    )

    assert coordination._cancel() == "conflict"
    assert len(cursor.execute.call_args_list) == 2


def test_begin_drain_endpoint_exposes_only_legal_transition(mock_client, mocker):
    transition = mocker.patch("ops.routers.coordination._transition", return_value="ok")

    response = mock_client.post("/coordination/begin-drain")

    assert response.status_code == 200
    assert response.json() == {"phase": "draining"}
    transition.assert_called_once_with("begin-drain")


@pytest.mark.parametrize(("result", "status_code"), [("conflict", 409), ("error", 503)])
def test_begin_drain_endpoint_maps_failures(mock_client, mocker, result, status_code):
    mocker.patch("ops.routers.coordination._transition", return_value=result)

    assert mock_client.post("/coordination/begin-drain").status_code == status_code


def test_status_route_is_registered(mock_client, mocker):
    """G6, Plan 162 Stage H. `test_status_serializes_timestamps` calls `_status`.

    That covers the serialisation and says nothing about the URL -- and this is
    the route `scripts/host_maintenance.py` polls as `GET /coordination/status`
    before it will proceed, so a rename here strands the host maintenance
    workflow rather than failing anything in this suite.
    """
    mocker.patch("ops.routers.coordination._status", return_value={"phase": "none"})

    response = mock_client.get("/coordination/status")

    assert response.status_code == 200
    assert response.json() == {"phase": "none"}


@pytest.mark.parametrize(
    ("path", "helper", "ok_phase"),
    [
        ("/coordination/cancel", "_cancel", "none"),
        ("/coordination/begin-validation", "_transition", "validating"),
    ],
)
def test_safe_lifecycle_endpoints(mock_client, mocker, path, helper, ok_phase):
    operation = mocker.patch(f"ops.routers.coordination.{helper}", return_value="ok")

    response = mock_client.post(path)

    assert response.status_code == 200
    assert response.json() == {"phase": ok_phase}
    if helper == "_transition":
        operation.assert_called_once_with("begin-validation")


@pytest.mark.parametrize(
    ("path", "helper", "result", "status_code"),
    [
        ("/coordination/cancel", "_cancel", "conflict", 409),
        ("/coordination/cancel", "_cancel", "error", 503),
        ("/coordination/begin-validation", "_transition", "conflict", 409),
        ("/coordination/begin-validation", "_transition", "error", 503),
    ],
)
def test_safe_lifecycle_endpoints_map_failures(
    mock_client, mocker, path, helper, result, status_code
):
    mocker.patch(f"ops.routers.coordination.{helper}", return_value=result)

    assert mock_client.post(path).status_code == status_code


def test_drain_status_aggregates_authoritative_state_without_transition(mock_client, mocker):
    state = {"phase": "draining", "scope": ["processing"]}
    mocker.patch("ops.routers.coordination._status", return_value=state)
    collect = mocker.patch(
        "ops.routers.coordination.collect_drain_status",
        return_value={"phase": "draining", "scope": ["processing"], "drained": True},
    )
    transition = mocker.patch("ops.routers.coordination._transition")

    response = mock_client.get("/coordination/drain-status")

    assert response.status_code == 200
    assert response.json()["drained"] is True
    collect.assert_called_once_with(state)
    transition.assert_not_called()


def test_release_status_returns_full_gate_evidence_without_transition(mock_client, mocker):
    state = {"phase": "validating", "kind": "host_maintenance"}
    mocker.patch("ops.routers.coordination._status", return_value=state)
    collect = mocker.patch(
        "ops.routers.coordination.collect_release_status",
        return_value={"release_ready": False, "blockers": ["container_health"], "gates": []},
    )
    transition = mocker.patch("ops.routers.coordination._transition")

    response = mock_client.get("/coordination/release-status")

    assert response.status_code == 200
    assert response.json()["blockers"] == ["container_health"]
    collect.assert_called_once_with(state)
    transition.assert_not_called()


def _host_evidence_payload(generation=7, gates=None):
    gates = gates or {
        name: {"verdict": "pass", "reason": "verified"}
        for name in coordination.HOST_VALIDATION_GATES
    }
    return {
        "generation": generation,
        "gates": gates,
        "evidence_digests": {"preflight": "a" * 64, "manifest": "b" * 64},
    }


def _complete_state():
    return {
        "phase": "validating",
        "generation": 7,
        "kind": "host_maintenance",
        "requested_by": "operator",
    }


def test_complete_refuses_without_operator_confirmation(mock_cursor_context, mocker):
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = _complete_state()
    release = mocker.patch("ops.routers.coordination.collect_release_status")

    result, evidence = coordination._complete(coordination.CompletionRequest())

    assert (result, evidence) == ("conflict", {"failing_gates": ["operator_confirmation"]})
    release.assert_not_called()


def test_complete_endpoint_returns_named_conflicts(mock_client, mocker):
    mocker.patch(
        "ops.routers.coordination._complete",
        return_value=("conflict", {"failing_gates": ["operator_confirmation"]}),
    )

    response = mock_client.post("/coordination/complete", json={"confirm_complete": False})

    assert response.status_code == 409
    assert response.json()["detail"]["failing_gates"] == ["operator_confirmation"]


@pytest.mark.parametrize("phase", ["none", "requested", "draining", "active"])
def test_complete_refuses_wrong_phase(mock_cursor_context, phase):
    _, cursor = mock_cursor_context
    state = _complete_state()
    state["phase"] = phase
    cursor.fetchone.side_effect = [state, None] if phase == "none" else [state]

    result, evidence = coordination._complete(
        coordination.CompletionRequest(
            confirm_complete=True, generation=7, manifest_sha256="a" * 64
        )
    )

    expected_gate = "completion_receipt" if phase == "none" else "coordination_expected"
    assert (result, evidence) == ("conflict", {"failing_gates": [expected_gate]})


def test_complete_refuses_failing_stack_gate(mock_cursor_context, mocker):
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = _complete_state()
    mocker.patch(
        "ops.routers.coordination.collect_release_status",
        return_value={"blockers": ["container_health"], "gates": []},
    )

    result, evidence = coordination._complete(
        coordination.CompletionRequest(
            confirm_complete=True, generation=7, manifest_sha256="a" * 64
        )
    )

    assert result == "conflict"
    assert evidence["failing_gates"] == ["container_health"]


def test_complete_refuses_without_passing_host_evidence(mock_cursor_context, mocker):
    _, cursor = mock_cursor_context
    cursor.fetchone.side_effect = [_complete_state()]
    cursor.fetchall.return_value = []
    mocker.patch(
        "ops.routers.coordination.collect_release_status",
        return_value={"blockers": [], "gates": []},
    )

    result, evidence = coordination._complete(
        coordination.CompletionRequest(confirm_complete=True)
    )

    assert result == "conflict"
    assert set(evidence["failing_gates"]) == set(coordination.HOST_VALIDATION_GATES)


def test_complete_succeeds_with_both_validation_halves(mock_cursor_context, mocker):
    _, cursor = mock_cursor_context
    cursor.fetchone.side_effect = [_complete_state(), {"generation": 7}]
    cursor.fetchall.return_value = [{"gate_results": _host_evidence_payload()["gates"]}]
    mocker.patch(
        "ops.routers.coordination.collect_release_status",
        return_value={"blockers": [], "gates": []},
    )

    result, completed = coordination._complete(
        coordination.CompletionRequest(
            confirm_complete=True, generation=7, manifest_sha256="a" * 64
        )
    )

    assert (result, completed) == ("ok", {"phase": "none", "generation": 7})
    update_sql = cursor.execute.call_args_list[-3].args[0]
    assert "kind = NULL" in update_sql
    assert cursor.execute.call_args_list[-1].args[1] == (
        7, "validating", "none", "host_maintenance", "operator"
    )


def test_complete_replay_confirms_matching_receipt(mock_cursor_context):
    _, cursor = mock_cursor_context
    state = _complete_state()
    state["phase"] = "none"
    cursor.fetchone.side_effect = [state, {"generation": 7}]

    result, completed = coordination._complete(
        coordination.CompletionRequest(
            confirm_complete=True, generation=7, manifest_sha256="a" * 64
        )
    )

    assert (result, completed) == ("ok", {"phase": "none", "generation": 7})


def test_host_evidence_rejects_missing_gate(mock_client):
    payload = _host_evidence_payload()
    payload["gates"].pop(next(iter(payload["gates"])))

    response = mock_client.post("/coordination/host-evidence", json=payload)

    assert response.status_code == 422


def test_host_evidence_rejects_stale_generation(mock_cursor_context):
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = {
        "phase": "validating",
        "generation": 8,
        "kind": "host_maintenance",
        "requested_by": "operator",
    }

    result, evidence = coordination._submit_host_evidence(
        coordination.HostEvidenceRequest(**_host_evidence_payload())
    )

    assert result == "stale"
    assert evidence == {"reason": "evidence generation is stale"}
    assert all(call.args[0] != INSERT_COORDINATION_RELEASE_EVIDENCE
               for call in cursor.execute.call_args_list)


def test_host_evidence_is_accepted_and_returned(mock_cursor_context):
    _, cursor = mock_cursor_context
    cursor.fetchone.side_effect = [
        {
            "phase": "validating",
            "generation": 7,
            "kind": "host_maintenance",
            "requested_by": "operator",
        },
        {"evidence_id": 4, "submitted_at": datetime(2026, 8, 26)},
    ]

    result, evidence = coordination._submit_host_evidence(
        coordination.HostEvidenceRequest(**_host_evidence_payload())
    )

    assert result == "ok"
    assert evidence["evidence_id"] == 4
    assert evidence["actor"] == "operator"
    sql, params = cursor.execute.call_args_list[-1].args
    assert sql == INSERT_COORDINATION_RELEASE_EVIDENCE
    assert params[0:2] == (7, "operator")


def test_host_evidence_failed_insert_leaves_no_partial_state(mock_cursor_context):
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = {
        "phase": "validating",
        "generation": 7,
        "kind": "host_maintenance",
        "requested_by": "operator",
    }
    cursor.execute.side_effect = [None, None, RuntimeError("write failed")]

    result, evidence = coordination._submit_host_evidence(
        coordination.HostEvidenceRequest(**_host_evidence_payload())
    )

    assert (result, evidence) == ("error", None)
    sql_calls = [call.args[0] for call in cursor.execute.call_args_list]
    assert all("UPDATE coordination_state" not in sql for sql in sql_calls)


def test_authorize_uses_locked_current_generation_confirming_read(mock_cursor_context, mocker):
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = {
        "kind": "service_maintenance",
        "phase": "draining",
        "generation": 8,
        "requested_by": "operator",
        "scope": ["processing"],
    }
    cursor.rowcount = 1
    evidence = {"drained": True, "blockers": [], "sources": []}
    collect = mocker.patch("ops.routers.coordination.collect_drain_status", return_value=evidence)

    result, returned = coordination._authorize()

    assert (result, returned) == ("ok", evidence)
    collect.assert_called_once()
    sql, params = cursor.execute.call_args_list[-2].args
    assert "phase = 'active'" in sql
    assert "phase = 'draining' AND generation = %s" in sql
    assert params == (8,)
    assert cursor.execute.call_args_list[-1].args[1] == (
        8,
        "draining",
        "active",
        "service_maintenance",
        "operator",
    )


def test_authorize_does_not_write_when_confirming_read_has_blockers(mock_cursor_context, mocker):
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = {
        "kind": "service_maintenance",
        "phase": "draining",
        "generation": 8,
        "requested_by": "operator",
        "scope": ["processing"],
    }
    evidence = {
        "drained": False,
        "blockers": ["airflow_gate_observations"],
        "sources": [],
    }
    mocker.patch("ops.routers.coordination.collect_drain_status", return_value=evidence)

    assert coordination._authorize() == ("blocked", evidence)
    assert len(cursor.execute.call_args_list) == 2


@pytest.mark.parametrize(
    ("result", "status_code"),
    [("ok", 200), ("blocked", 409), ("conflict", 409), ("error", 503)],
)
def test_authorize_endpoint_maps_result(mock_client, mocker, result, status_code):
    evidence = {"drained": result == "ok", "blockers": [], "sources": []}
    mocker.patch(
        "ops.routers.coordination._authorize",
        return_value=(result, evidence if result in {"ok", "blocked"} else None),
    )

    response = mock_client.post("/coordination/authorize")

    assert response.status_code == status_code
    if result == "ok":
        assert response.json()["phase"] == "active"


def _request_payload(**overrides):
    payload = {
        "kind": "service_maintenance",
        "targets": ["processing"],
        "requested_by": "operator",
        "reason": "deploy processing",
    }
    payload.update(overrides)
    return payload


def test_request_writes_expanded_immutable_scope(mock_cursor_context):
    _, cursor = mock_cursor_context
    cursor.fetchone.side_effect = [("none",), (7,)]
    payload = coordination.CoordinationRequest(**_request_payload())

    result, requested = coordination._request(payload)

    assert result == "ok"
    assert requested == {
        "kind": "service_maintenance",
        "phase": "requested",
        "targets": ["processing"],
        "scope": ["processing"],
    }
    sql, params = cursor.execute.call_args_list[-2].args
    assert "targets = %s::jsonb" in sql
    assert "scope = %s::jsonb" in sql
    assert json.loads(params[1]) == ["processing"]
    assert json.loads(params[2]) == ["processing"]
    assert cursor.execute.call_args_list[-1].args[1] == (
        7,
        "none",
        "requested",
        "service_maintenance",
        "operator",
    )


def test_event_failure_rolls_back_state_mutation(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.side_effect = [("none",), (7,)]

    def fail_event(sql, params=None):
        if sql == INSERT_COORDINATION_STATE_EVENT:
            raise RuntimeError("history unavailable")

    cursor.execute.side_effect = fail_event
    payload = coordination.CoordinationRequest(**_request_payload())

    assert coordination._request(payload) == ("error", "history unavailable")
    conn.commit.assert_not_called()
    conn.rollback.assert_called_once()


def test_request_refuses_active_coordination_without_writing(mock_cursor_context):
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = ("draining", 7, "service_maintenance")

    result, requested = coordination._request(
        coordination.CoordinationRequest(**_request_payload())
    )

    assert (result, requested) == ("conflict", None)
    assert len(cursor.execute.call_args_list) == 2


@pytest.mark.parametrize(
    "overrides",
    [
        {"kind": "future_kind"},
        {"targets": ["future-service"]},
        {"targets": ["processing", "processing"]},
        {"targets": ["host"]},
        {"kind": "host_maintenance", "targets": ["processing"]},
        {"kind": "host_maintenance", "targets": ["host", "postgres"]},
    ],
)
def test_request_rejects_invalid_kind_or_scope_before_database(mock_cursor_context, overrides):
    _, cursor = mock_cursor_context
    payload = coordination.CoordinationRequest(**_request_payload(**overrides))

    assert coordination._request(payload) == ("invalid", None)
    cursor.execute.assert_not_called()


def test_host_request_selects_every_surface(mock_cursor_context):
    _, cursor = mock_cursor_context
    cursor.fetchone.side_effect = [("none",), (9,)]
    payload = coordination.CoordinationRequest(
        **_request_payload(kind="host_maintenance", targets=["host"])
    )

    result, requested = coordination._request(payload)

    assert result == "ok"
    assert requested["targets"] == ["host"]
    assert "host" in requested["scope"]
    assert "database" in requested["scope"]


def test_request_endpoint_returns_expanded_scope(mock_client, mocker):
    mocker.patch(
        "ops.routers.coordination._request",
        return_value=(
            "ok",
            {
                "kind": "service_maintenance",
                "phase": "requested",
                "targets": ["processing"],
                "scope": ["processing"],
            },
        ),
    )

    response = mock_client.post("/coordination/request", json=_request_payload())

    assert response.status_code == 200
    assert response.json()["scope"] == ["processing"]


@pytest.mark.parametrize(
    ("result", "status_code"),
    [("conflict", 409), ("invalid", 422), ("unavailable", 503), ("error", 500)],
)
def test_request_endpoint_maps_failures(mock_client, mocker, result, status_code):
    mocker.patch("ops.routers.coordination._request", return_value=(result, None))

    response = mock_client.post("/coordination/request", json=_request_payload())

    assert response.status_code == status_code


def test_a_refused_request_names_the_cause_instead_of_blaming_the_database(
    mock_client, mocker
):
    """`/coordination/request` masked its failures exactly as `/deploy/start`
    did, and expands the same contract, so `service_maintenance` on `dashboard`
    hit the same constraint by the same route. Plan 162 Stage K.
    """
    violation = (
        'new row for relation "coordination_state" violates check constraint '
        '"coordination_state_check"'
    )
    mocker.patch(
        "ops.routers.coordination._request", return_value=("error", violation)
    )

    response = mock_client.post("/coordination/request", json=_request_payload())

    assert response.status_code == 500
    detail = response.json()["detail"]
    assert "coordination_state_check" in detail
    assert "Database unavailable" not in detail


def test_migration_has_single_row_kind_phase_and_named_target_contract():
    sql = Path("db/migrations/V043__coordination_state.sql").read_text(encoding="utf-8")

    assert "CHECK (id = 1)" in sql
    for phase in ("none", "requested", "draining", "active", "validating"):
        assert f"'{phase}'" in sql
    for kind in ("deploy", "service_maintenance", "host_maintenance"):
        assert f"'{kind}'" in sql
    assert "targets <> '[]'::jsonb" in sql
    assert "generation bigint NOT NULL DEFAULT 0" in sql
    assert "CREATE TABLE public.coordination_gate_observations" in sql
    assert "PRIMARY KEY (generation, dag_id, run_id)" in sql


def test_v050_lets_a_record_name_a_target_while_pausing_no_surface():
    """V043's non-empty-scope half is superseded, and this file was asserting
    it. `dashboard` and `pgadmin` map to no surfaces by design, so requiring a
    non-empty scope made those two impossible to deploy alone; the target stays
    required, because an active record must still name what it coordinates.

    Text-matching a migration says only what the file contains. What the
    database accepts is asserted against a real Postgres, one service at a
    time, in tests/integration/ops/test_deploy_intent.py.
    """
    sql = Path(
        "db/migrations/V050__coordination_state_allows_empty_scope.sql"
    ).read_text(encoding="utf-8")

    assert "DROP CONSTRAINT coordination_state_check" in sql
    assert "ADD CONSTRAINT coordination_state_check" in sql
    assert "targets <> '[]'::jsonb" in sql
    assert "scope <> '[]'::jsonb" not in sql


def test_coordination_event_migration_is_append_only_and_archiver_accessible():
    sql = Path("db/migrations/V044__coordination_state_events.sql").read_text(encoding="utf-8")

    assert "CREATE TABLE staging.coordination_state_events" in sql
    assert "event_id bigserial PRIMARY KEY" in sql
    for column in (
        "generation",
        "prior_phase",
        "phase",
        "kind",
        "actor",
        "event_at",
    ):
        assert column in sql
    assert "UPDATE staging.coordination_state_events" not in sql
    assert "SELECT, INSERT, DELETE ON staging.coordination_state_events" in sql
    assert "coordination_state_events_event_id_seq" in sql


def test_host_evidence_migration_is_append_only_and_archiver_accessible():
    sql = Path("db/migrations/V045__coordination_release_evidence.sql").read_text(encoding="utf-8")

    assert "CREATE TABLE staging.coordination_release_evidence" in sql
    assert "evidence_id bigserial PRIMARY KEY" in sql
    for column in ("generation", "actor", "submitted_at", "gate_results", "evidence_digests"):
        assert column in sql
    assert "UPDATE staging.coordination_release_evidence" not in sql
    assert "SELECT, INSERT, DELETE ON staging.coordination_release_evidence" in sql


def test_every_declared_transition_has_a_durable_event_phase():
    sql = Path("db/migrations/V044__coordination_state_events.sql").read_text(encoding="utf-8")

    phases = {
        phase for transition in coordination._TRANSITIONS.values() for phase in transition[:2]
    }
    phases.update({"none", "requested"})
    for phase in phases:
        assert f"'{phase}'" in sql
