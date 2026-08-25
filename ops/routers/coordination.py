"""Scoped operational-coordination state (Plan 142 Stage 1)."""

import json
from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ops.coordination_contract import HOST_TARGET, expand_targets
from shared.db import db_cursor

router = APIRouter(prefix="/coordination")

COORDINATION_LOCK_ID = 142
KINDS = frozenset({"deploy", "service_maintenance", "host_maintenance"})

_TRANSITIONS = {
    "begin-drain": ("requested", "draining", "draining_at"),
    "authorize": ("draining", "active", "active_at"),
    "begin-validation": ("active", "validating", "validating_at"),
    "complete": ("validating", "none", "completed_at"),
}

_STATUS_SQL = """
    SELECT kind, phase, requested_by, reason, targets, scope, requested_at,
           draining_at, active_at, validating_at, completed_at, expected_work,
           manifest_location, operator_notes, updated_at
      FROM coordination_state
     WHERE id = 1
"""


class CoordinationRequest(BaseModel):
    kind: str
    targets: list[str] = Field(min_length=1)
    requested_by: str = Field(min_length=1, max_length=200)
    reason: str = Field(min_length=1, max_length=1000)
    expected_work: list[str] = Field(default_factory=list)
    manifest_location: str | None = Field(default=None, max_length=1000)
    operator_notes: str | None = Field(default=None, max_length=4000)


def _iso(value: Any) -> Any:
    return value.isoformat() if isinstance(value, datetime) else value


def _status() -> dict[str, Any]:
    """Return the authoritative coordination record; never synthesize none."""
    try:
        with db_cursor(error_context="Coordination-Status", dict_cursor=True) as cur:
            cur.execute(_STATUS_SQL)
            row = cur.fetchone()
    except Exception:
        raise HTTPException(status_code=503, detail="Database unavailable.")

    if row is None:
        raise HTTPException(status_code=503, detail="Coordination state is missing.")
    return {key: _iso(value) for key, value in dict(row).items()}


def _request(payload: CoordinationRequest) -> tuple[str, dict[str, Any] | None]:
    """Create one immutable scoped request while the coordination row is idle."""
    if payload.kind not in KINDS:
        return "invalid", None

    target_set = set(payload.targets)
    if len(target_set) != len(payload.targets):
        return "invalid", None
    if payload.kind == "host_maintenance":
        if target_set != {HOST_TARGET}:
            return "invalid", None
    elif HOST_TARGET in target_set:
        return "invalid", None

    try:
        expanded_targets, scope = expand_targets(target_set)
    except ValueError:
        return "invalid", None

    targets_json = sorted(expanded_targets)
    scope_json = sorted(scope)
    try:
        with db_cursor(error_context="Coordination-Request") as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (COORDINATION_LOCK_ID,))
            cur.execute("SELECT phase FROM coordination_state WHERE id = 1")
            row = cur.fetchone()
            if row is None:
                return "error", None
            if row[0] != "none":
                return "conflict", None
            cur.execute(
                """UPDATE coordination_state
                      SET kind = %s, phase = 'requested', targets = %s::jsonb,
                          scope = %s::jsonb, requested_by = %s, reason = %s,
                          requested_at = now(), draining_at = NULL,
                          active_at = NULL, validating_at = NULL,
                          completed_at = NULL, expected_work = %s::jsonb,
                          manifest_location = %s, operator_notes = %s,
                          updated_at = now()
                    WHERE id = 1""",
                (
                    payload.kind,
                    json.dumps(targets_json),
                    json.dumps(scope_json),
                    payload.requested_by,
                    payload.reason,
                    json.dumps(payload.expected_work),
                    payload.manifest_location,
                    payload.operator_notes,
                ),
            )
    except Exception:
        return "error", None

    return "ok", {
        "kind": payload.kind,
        "phase": "requested",
        "targets": targets_json,
        "scope": scope_json,
    }


def _transition(operation: str) -> str:
    """Apply a legal transition. Authorization remains private until drained."""
    source, target, timestamp_column = _TRANSITIONS[operation]
    try:
        with db_cursor(error_context=f"Coordination-{operation}") as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (COORDINATION_LOCK_ID,))
            cur.execute("SELECT phase FROM coordination_state WHERE id = 1")
            row = cur.fetchone()
            if row is None:
                return "error"
            if row[0] != source:
                return "conflict"

            if target == "none":
                cur.execute(
                    f"""UPDATE coordination_state
                            SET kind = NULL, phase = 'none', targets = '[]'::jsonb,
                                scope = '[]'::jsonb, {timestamp_column} = now(),
                                updated_at = now()
                          WHERE id = 1"""
                )
            else:
                cur.execute(
                    f"""UPDATE coordination_state
                            SET phase = %s, {timestamp_column} = now(), updated_at = now()
                          WHERE id = 1""",
                    (target,),
                )
        return "ok"
    except Exception:
        return "error"


def _cancel() -> str:
    """Cancel before mutation authorization; never auto-release active work."""
    try:
        with db_cursor(error_context="Coordination-Cancel") as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (COORDINATION_LOCK_ID,))
            cur.execute("SELECT phase FROM coordination_state WHERE id = 1")
            row = cur.fetchone()
            if row is None:
                return "error"
            if row[0] not in {"requested", "draining"}:
                return "conflict"
            cur.execute(
                """UPDATE coordination_state
                      SET kind = NULL, phase = 'none', targets = '[]'::jsonb,
                          scope = '[]'::jsonb, completed_at = now(), updated_at = now()
                    WHERE id = 1"""
            )
        return "ok"
    except Exception:
        return "error"


@router.get("/status")
def coordination_status() -> dict[str, Any]:
    return _status()


@router.post("/request")
def request_coordination(payload: CoordinationRequest) -> dict[str, Any]:
    result, requested = _request(payload)
    if result == "ok" and requested is not None:
        return requested
    if result == "conflict":
        raise HTTPException(status_code=409, detail="Coordination is already active.")
    if result == "invalid":
        raise HTTPException(status_code=422, detail="Invalid coordination scope.")
    raise HTTPException(status_code=503, detail="Database unavailable.")


# Drain authorization, validation, and release routes are added only with
# their evidence guards. Exposing an unguarded
# draining->active endpoint would turn a state machine into false authority.
