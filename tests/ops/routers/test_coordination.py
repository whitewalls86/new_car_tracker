"""Plan 142 Stage 1 coordination-state core."""

import json
from datetime import datetime
from pathlib import Path

import pytest

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
    assert "INSERT INTO staging.coordination_state_events" in event_sql
    assert event_params == (7, source, target, "service_maintenance", "operator")


def test_complete_clears_kind_targets_and_scope(mock_cursor_context):
    _, cursor = mock_cursor_context
    cursor.fetchone.side_effect = [
        ("validating", 7, "host_maintenance", "operator"),
        (7,),
    ]

    assert coordination._transition("complete") == "ok"

    sql = cursor.execute.call_args_list[-2].args[0]
    assert "kind = NULL" in sql
    assert "targets = '[]'::jsonb" in sql
    assert "scope = '[]'::jsonb" in sql
    assert cursor.execute.call_args_list[-1].args[1] == (
        7,
        "validating",
        "none",
        "host_maintenance",
        "operator",
    )


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


def test_release_route_remains_private_until_validation_guard_exists(mock_client):
    assert mock_client.post("/coordination/complete").status_code == 404


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
        if "INSERT INTO staging.coordination_state_events" in sql:
            raise RuntimeError("history unavailable")

    cursor.execute.side_effect = fail_event
    payload = coordination.CoordinationRequest(**_request_payload())

    assert coordination._request(payload) == ("error", None)
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
    [("conflict", 409), ("invalid", 422), ("error", 503)],
)
def test_request_endpoint_maps_failures(mock_client, mocker, result, status_code):
    mocker.patch("ops.routers.coordination._request", return_value=(result, None))

    response = mock_client.post("/coordination/request", json=_request_payload())

    assert response.status_code == status_code


def test_migration_has_single_row_kind_phase_and_nonempty_scope_contract():
    sql = Path("db/migrations/V043__coordination_state.sql").read_text()

    assert "CHECK (id = 1)" in sql
    for phase in ("none", "requested", "draining", "active", "validating"):
        assert f"'{phase}'" in sql
    for kind in ("deploy", "service_maintenance", "host_maintenance"):
        assert f"'{kind}'" in sql
    assert "scope <> '[]'::jsonb" in sql
    assert "generation bigint NOT NULL DEFAULT 0" in sql
    assert "CREATE TABLE public.coordination_gate_observations" in sql
    assert "PRIMARY KEY (generation, dag_id, run_id)" in sql


def test_coordination_event_migration_is_append_only_and_archiver_accessible():
    sql = Path("db/migrations/V044__coordination_state_events.sql").read_text()

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


def test_every_declared_transition_has_a_durable_event_phase():
    sql = Path("db/migrations/V044__coordination_state_events.sql").read_text()

    phases = {
        phase for transition in coordination._TRANSITIONS.values() for phase in transition[:2]
    }
    phases.update({"none", "requested"})
    for phase in phases:
        assert f"'{phase}'" in sql
