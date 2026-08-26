"""Scoped operational-coordination state (Plan 142 Stage 1)."""

import json
import logging
from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ops.coordination_contract import HOST_TARGET, expand_targets
from ops.coordination_drain import collect_drain_status
from ops.coordination_release import collect_release_status
from scripts.host_maintenance import HOST_VALIDATION_GATES
from shared.db import db_cursor
from shared.job_counter import job_snapshot

router = APIRouter(prefix="/coordination")
logger = logging.getLogger(__name__)

COORDINATION_LOCK_ID = 142
KINDS = frozenset({"deploy", "service_maintenance", "host_maintenance"})

_TRANSITIONS = {
    "begin-drain": ("requested", "draining", "draining_at"),
    "authorize": ("draining", "active", "active_at"),
    "begin-validation": ("active", "validating", "validating_at"),
    "complete": ("validating", "none", "completed_at"),
}

_STATUS_SQL = """
    SELECT kind, phase, generation, requested_by, reason, targets, scope, requested_at,
           draining_at, active_at, validating_at, completed_at, expected_work,
           manifest_location, operator_notes, updated_at
      FROM coordination_state
     WHERE id = 1
"""

_EVENT_INSERT_SQL = """
    INSERT INTO staging.coordination_state_events
        (generation, prior_phase, phase, kind, actor)
    VALUES (%s, %s, %s, %s, %s)
"""

_HOST_EVIDENCE_INSERT_SQL = """
    INSERT INTO staging.coordination_release_evidence
        (generation, actor, gate_results, evidence_digests)
    VALUES (%s, %s, %s::jsonb, %s::jsonb)
    RETURNING evidence_id, submitted_at
"""


class CoordinationRequest(BaseModel):
    kind: str
    targets: list[str] = Field(min_length=1)
    requested_by: str = Field(min_length=1, max_length=200)
    reason: str = Field(min_length=1, max_length=1000)
    expected_work: list[str] = Field(default_factory=list)
    manifest_location: str | None = Field(default=None, max_length=1000)
    operator_notes: str | None = Field(default=None, max_length=4000)


class HostEvidenceRequest(BaseModel):
    generation: int = Field(ge=1)
    gates: dict[str, dict[str, Any]]
    evidence_digests: dict[str, str]


class CompletionRequest(BaseModel):
    confirm_complete: bool = False


def _validate_host_evidence(payload: HostEvidenceRequest) -> str | None:
    """Return a refusal reason unless this is a complete host-gate bundle."""
    expected = set(HOST_VALIDATION_GATES)
    if set(payload.gates) != expected:
        missing = sorted(expected - set(payload.gates))
        extra = sorted(set(payload.gates) - expected)
        details = []
        if missing:
            details.append(f"missing gates: {', '.join(missing)}")
        if extra:
            details.append(f"unknown gates: {', '.join(extra)}")
        return "; ".join(details)
    for name, result in payload.gates.items():
        if result.get("verdict") not in {"pass", "fail", "unknown"}:
            return f"invalid verdict for {name}"
        if not isinstance(result.get("reason"), str) or not result["reason"]:
            return f"missing reason for {name}"
    if set(payload.evidence_digests) != {"preflight", "manifest"}:
        return "evidence digests must name preflight and manifest"
    if any(len(value) != 64 for value in payload.evidence_digests.values()):
        return "evidence digests must be SHA-256 values"
    return None


def _submit_host_evidence(payload: HostEvidenceRequest) -> tuple[str, dict[str, Any] | None]:
    """Durably record one complete validation bundle for its live generation."""
    invalid = _validate_host_evidence(payload)
    if invalid:
        return "invalid", {"reason": invalid}
    try:
        with db_cursor(error_context="Coordination-HostEvidence", dict_cursor=True) as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (COORDINATION_LOCK_ID,))
            cur.execute(
                """SELECT phase, generation, kind, requested_by
                     FROM coordination_state WHERE id = 1"""
            )
            row = cur.fetchone()
            if row is None:
                return "error", None
            state = dict(row)
            if state["phase"] != "validating" or state["kind"] != "host_maintenance":
                return "conflict", {"reason": "coordination is not validating host maintenance"}
            if state["generation"] != payload.generation:
                return "stale", {"reason": "evidence generation is stale"}
            cur.execute(
                _HOST_EVIDENCE_INSERT_SQL,
                (
                    payload.generation,
                    state["requested_by"],
                    json.dumps(payload.gates, sort_keys=True),
                    json.dumps(payload.evidence_digests, sort_keys=True),
                ),
            )
            inserted = cur.fetchone()
            if inserted is None:
                return "error", None
    except Exception:
        return "error", None
    return "ok", {
        "evidence_id": inserted["evidence_id"],
        "generation": payload.generation,
        "actor": state["requested_by"],
        "submitted_at": _iso(inserted["submitted_at"]),
        "gates": payload.gates,
        "evidence_digests": payload.evidence_digests,
    }


def _iso(value: Any) -> Any:
    return value.isoformat() if isinstance(value, datetime) else value


def record_transition_event(
    cur,
    *,
    generation: int,
    prior_phase: str,
    phase: str,
    kind: str,
    actor: str,
) -> None:
    """Append the transition using the state mutation's open transaction."""
    actor = actor.strip()
    if not actor:
        raise ValueError("coordination transition actor is empty")
    cur.execute(
        _EVENT_INSERT_SQL,
        (generation, prior_phase, phase, kind, actor),
    )


def log_transition(*, generation: int, prior_phase: str, phase: str, kind: str) -> None:
    logger.info(
        "coordination transition %s -> %s",
        prior_phase,
        phase,
        extra={
            "generation": generation,
            "prior_phase": prior_phase,
            "phase": phase,
            "kind": kind,
        },
    )


def log_refusal(
    *,
    operation: str,
    generation: int,
    prior_phase: str,
    phase: str,
    kind: str | None,
    reason: str,
) -> None:
    logger.warning(
        "coordination %s refused: %s",
        operation,
        reason,
        extra={
            "generation": generation,
            "prior_phase": prior_phase,
            "phase": phase,
            "kind": kind,
        },
    )


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
            cur.execute(
                """SELECT phase, generation, kind
                     FROM coordination_state WHERE id = 1"""
            )
            row = cur.fetchone()
            if row is None:
                return "error", None
            if row[0] != "none":
                log_refusal(
                    operation="request",
                    generation=row[1],
                    prior_phase=row[0],
                    phase="requested",
                    kind=row[2],
                    reason="coordination is already active",
                )
                return "conflict", None
            cur.execute(
                """UPDATE coordination_state
                      SET kind = %s, phase = 'requested', generation = generation + 1,
                          targets = %s::jsonb,
                          scope = %s::jsonb, requested_by = %s, reason = %s,
                          requested_at = now(), draining_at = NULL,
                          active_at = NULL, validating_at = NULL,
                          completed_at = NULL, expected_work = %s::jsonb,
                          manifest_location = %s, operator_notes = %s,
                          updated_at = now()
                    WHERE id = 1
                RETURNING generation""",
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
            changed = cur.fetchone()
            if changed is None:
                return "error", None
            generation = changed[0]
            record_transition_event(
                cur,
                generation=generation,
                prior_phase="none",
                phase="requested",
                kind=payload.kind,
                actor=payload.requested_by,
            )
    except Exception:
        return "error", None

    log_transition(
        generation=generation,
        prior_phase="none",
        phase="requested",
        kind=payload.kind,
    )

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
            cur.execute(
                """SELECT phase, generation, kind, requested_by
                     FROM coordination_state WHERE id = 1"""
            )
            row = cur.fetchone()
            if row is None:
                return "error"
            if row[0] != source:
                log_refusal(
                    operation=operation,
                    generation=row[1],
                    prior_phase=row[0],
                    phase=target,
                    kind=row[2],
                    reason=f"expected phase {source}",
                )
                return "conflict"

            prior_phase, generation, kind, actor = row

            if target == "none":
                cur.execute(
                    f"""UPDATE coordination_state
                            SET kind = NULL, phase = 'none', targets = '[]'::jsonb,
                                scope = '[]'::jsonb, {timestamp_column} = now(),
                                updated_at = now()
                          WHERE id = 1
                      RETURNING generation"""
                )
            else:
                cur.execute(
                    f"""UPDATE coordination_state
                            SET phase = %s, {timestamp_column} = now(), updated_at = now()
                          WHERE id = 1
                      RETURNING generation""",
                    (target,),
                )
            changed = cur.fetchone()
            if changed is None:
                return "error"
            generation = changed[0]
            record_transition_event(
                cur,
                generation=generation,
                prior_phase=prior_phase,
                phase=target,
                kind=kind,
                actor=actor,
            )
        log_transition(
            generation=generation,
            prior_phase=prior_phase,
            phase=target,
            kind=kind,
        )
        return "ok"
    except Exception:
        return "error"


def _complete(payload: CompletionRequest) -> tuple[str, dict[str, Any] | None]:
    """Release only after fresh stack and durable host validation evidence."""
    try:
        with db_cursor(error_context="Coordination-Complete", dict_cursor=True) as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (COORDINATION_LOCK_ID,))
            cur.execute(
                """SELECT phase, generation, kind, requested_by
                     FROM coordination_state WHERE id = 1"""
            )
            row = cur.fetchone()
            if row is None:
                return "error", None
            state = dict(row)
            if state["phase"] != "validating" or state["kind"] != "host_maintenance":
                return "conflict", {"failing_gates": ["coordination_expected"]}
            if not payload.confirm_complete:
                return "conflict", {"failing_gates": ["operator_confirmation"]}

            stack = collect_release_status(state)
            stack_blockers = stack["blockers"]
            if stack_blockers:
                return "conflict", {"failing_gates": stack_blockers, "release": stack}

            cur.execute(
                """SELECT gate_results
                     FROM staging.coordination_release_evidence
                    WHERE generation = %s
                    ORDER BY evidence_id DESC""",
                (state["generation"],),
            )
            evidence_rows = cur.fetchall()
            host_evidence_passes = any(
                isinstance(row["gate_results"], dict)
                and set(row["gate_results"]) == set(HOST_VALIDATION_GATES)
                and all(
                    row["gate_results"][gate].get("verdict") == "pass"
                    for gate in HOST_VALIDATION_GATES
                )
                for row in evidence_rows
            )
            if not host_evidence_passes:
                return "conflict", {"failing_gates": list(HOST_VALIDATION_GATES)}

            cur.execute(
                """UPDATE coordination_state
                      SET kind = NULL, phase = 'none', targets = '[]'::jsonb,
                          scope = '[]'::jsonb, completed_at = now(), updated_at = now()
                    WHERE id = 1
                RETURNING generation"""
            )
            changed = cur.fetchone()
            if changed is None:
                return "error", None
            record_transition_event(
                cur,
                generation=changed["generation"],
                prior_phase="validating",
                phase="none",
                kind="host_maintenance",
                actor=state["requested_by"],
            )
    except Exception:
        return "error", None
    log_transition(
        generation=changed["generation"],
        prior_phase="validating",
        phase="none",
        kind="host_maintenance",
    )
    return "ok", {"phase": "none", "generation": changed["generation"]}


def _cancel() -> str:
    """Cancel before mutation authorization; never auto-release active work."""
    try:
        with db_cursor(error_context="Coordination-Cancel") as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (COORDINATION_LOCK_ID,))
            cur.execute(
                """SELECT phase, generation, kind, requested_by
                     FROM coordination_state WHERE id = 1"""
            )
            row = cur.fetchone()
            if row is None:
                return "error"
            if row[0] not in {"requested", "draining"}:
                log_refusal(
                    operation="cancel",
                    generation=row[1],
                    prior_phase=row[0],
                    phase="none",
                    kind=row[2],
                    reason="coordination can no longer be cancelled",
                )
                return "conflict"
            prior_phase, _, kind, actor = row
            cur.execute(
                """UPDATE coordination_state
                      SET kind = NULL, phase = 'none', generation = generation + 1,
                          targets = '[]'::jsonb,
                          scope = '[]'::jsonb, completed_at = now(), updated_at = now()
                    WHERE id = 1
                RETURNING generation"""
            )
            changed = cur.fetchone()
            if changed is None:
                return "error"
            generation = changed[0]
            record_transition_event(
                cur,
                generation=generation,
                prior_phase=prior_phase,
                phase="none",
                kind=kind,
                actor=actor,
            )
        log_transition(
            generation=generation,
            prior_phase=prior_phase,
            phase="none",
            kind=kind,
        )
        return "ok"
    except Exception:
        return "error"


def _authorize() -> tuple[str, dict[str, Any] | None]:
    """Authorize only from a locked, current-generation confirming read."""
    try:
        with db_cursor(error_context="Coordination-Authorize", dict_cursor=True) as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (COORDINATION_LOCK_ID,))
            cur.execute(_STATUS_SQL)
            row = cur.fetchone()
            if row is None:
                return "error", None
            state = {key: _iso(value) for key, value in dict(row).items()}
            if state["phase"] != "draining":
                log_refusal(
                    operation="authorize",
                    generation=state["generation"],
                    prior_phase=state["phase"],
                    phase="active",
                    kind=state["kind"],
                    reason="coordination is not draining",
                )
                return "conflict", None

            evidence = collect_drain_status(state)
            if not evidence["drained"]:
                log_refusal(
                    operation="authorize",
                    generation=state["generation"],
                    prior_phase="draining",
                    phase="active",
                    kind=state["kind"],
                    reason="drain is not complete",
                )
                return "blocked", evidence

            cur.execute(
                """UPDATE coordination_state
                      SET phase = 'active', active_at = now(), updated_at = now()
                    WHERE id = 1 AND phase = 'draining' AND generation = %s""",
                (state["generation"],),
            )
            if cur.rowcount != 1:
                return "conflict", None
            record_transition_event(
                cur,
                generation=state["generation"],
                prior_phase="draining",
                phase="active",
                kind=state["kind"],
                actor=state["requested_by"],
            )
        log_transition(
            generation=state["generation"],
            prior_phase="draining",
            phase="active",
            kind=state["kind"],
        )
        return "ok", evidence
    except Exception:
        return "error", None


@router.get("/status")
def coordination_status() -> dict[str, Any]:
    return _status()


@router.get("/local-drain")
def local_drain_status() -> dict[str, Any]:
    """Expose ops maintenance work as one named drain-evidence source."""
    evidence = job_snapshot()
    return {"source": "ops_jobs", "known": True, **evidence}


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


@router.post("/begin-drain")
def begin_coordination_drain() -> dict[str, str]:
    result = _transition("begin-drain")
    if result == "ok":
        return {"phase": "draining"}
    if result == "conflict":
        raise HTTPException(status_code=409, detail="Coordination is not requested.")
    raise HTTPException(status_code=503, detail="Database unavailable.")


@router.get("/drain-status")
def coordination_drain_status() -> dict[str, Any]:
    return collect_drain_status(_status())


@router.get("/release-status")
def coordination_release_status() -> dict[str, Any]:
    """Expose the complete stack-release gate set without transitioning state."""
    return collect_release_status(_status())


@router.post("/host-evidence")
def submit_host_evidence(payload: HostEvidenceRequest) -> dict[str, Any]:
    """Store host validation proof without changing coordination state."""
    result, evidence = _submit_host_evidence(payload)
    if result == "ok" and evidence is not None:
        return evidence
    if result == "invalid":
        raise HTTPException(status_code=422, detail=evidence)
    if result in {"conflict", "stale"}:
        raise HTTPException(status_code=409, detail=evidence)
    raise HTTPException(status_code=503, detail="Host evidence could not be recorded.")


@router.post("/complete")
def complete_coordination(payload: CompletionRequest) -> dict[str, Any]:
    """End host maintenance only when both validation evidence halves pass."""
    result, completed = _complete(payload)
    if result == "ok" and completed is not None:
        return completed
    if result == "conflict":
        raise HTTPException(status_code=409, detail=completed)
    raise HTTPException(status_code=503, detail="Coordination could not be completed.")


@router.post("/authorize")
def authorize_coordination() -> dict[str, Any]:
    result, evidence = _authorize()
    if result == "ok":
        return {"phase": "active", "drain": evidence}
    if result == "blocked":
        raise HTTPException(status_code=409, detail={"reason": "not_drained", **evidence})
    if result == "conflict":
        raise HTTPException(status_code=409, detail="Coordination is not draining.")
    raise HTTPException(status_code=503, detail="Authorization evidence unavailable.")


@router.post("/cancel")
def cancel_coordination() -> dict[str, str]:
    result = _cancel()
    if result == "ok":
        return {"phase": "none"}
    if result == "conflict":
        raise HTTPException(status_code=409, detail="Coordination can no longer be cancelled.")
    raise HTTPException(status_code=503, detail="Database unavailable.")


@router.post("/begin-validation")
def begin_coordination_validation() -> dict[str, str]:
    result = _transition("begin-validation")
    if result == "ok":
        return {"phase": "validating"}
    if result == "conflict":
        raise HTTPException(status_code=409, detail="Coordination is not active.")
    raise HTTPException(status_code=503, detail="Database unavailable.")


# Release is added only with its validation evidence guard. Exposing an
# unguarded validating->none endpoint would turn state into false authority.
