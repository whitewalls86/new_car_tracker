"""
Deploy coordination API endpoints.
"""

import json
import logging
from typing import Any, Dict, NamedTuple

from fastapi import APIRouter, Body, HTTPException

from ops.coordination_contract import SERVICE_CONTRACTS, SURFACES, expand_targets
from ops.queries import (
    ACQUIRE_COORDINATION_LOCK,
    CLEAR_DEPLOY_INTENT,
    RELEASE_DEPLOY_COORDINATION,
    REQUEST_DEPLOY_COORDINATION,
    SELECT_COORDINATION_STATE_FOR_DEPLOY,
    SELECT_DEPLOY_INTENT_STATUS,
    SET_DEPLOY_INTENT,
)
from ops.routers.coordination import log_transition, record_transition_event
from shared.db import UNREACHABLE_ERRORS, db_cursor, db_failure_cause

logger = logging.getLogger("pipeline_ops")
router = APIRouter()

STALE_LOCK_MINUTES = 30
COORDINATION_LOCK_ID = 142
LEGACY_DEPLOY_TARGETS = tuple(sorted(SERVICE_CONTRACTS))
LEGACY_DEPLOY_SCOPE = tuple(sorted(SURFACES - {"host"}))


class IntentResult(NamedTuple):
    """What the intent write did, and -- when it failed -- what stopped it.

    ``detail`` is set only for ``error``, the status that means *the database
    answered and refused*. It exists because the alternative was a caller with
    nothing to say: before Plan 162 Stage 6c every failure here returned the
    bare string ``"error"`` and the route rendered it as 503 "Database
    unavailable", so a CHECK violation and an unreachable Postgres were
    indistinguishable in the response, in the log and to the operator. Two
    unrelated defects wore that face one day apart.
    """

    status: str
    detail: str | None = None


def _intent_status() -> Dict[str, Any]:
    """Return current deploy intent state plus in-flight counts."""

    try:
        with db_cursor(error_context="Intent-Status") as cur:
            cur.execute(SELECT_DEPLOY_INTENT_STATUS)
            row = cur.fetchone()

        if row:
            results = {
                "intent": row[0],
                "requested_at": row[1].isoformat() if row[1] else None,
                "requested_by": row[2],
                "number_running": row[3],
                "min_started_at": row[4].isoformat() if row[4] else None,
                "pause_long_jobs": row[5],
            }
        else:
            results = _no_intent()
    except Exception:
        results = _no_intent()

    return results


def _no_intent() -> Dict[str, Any]:
    """The shape returned when there is no row, or the read failed."""
    return {
        "intent": "none",
        "requested_at": None,
        "requested_by": None,
        # The column default. It only means anything while intent is 'pending',
        # so reporting it here is shape, not a claim about a deploy.
        "pause_long_jobs": True,
    }


def _set_intent(
    caller: str, pause_long_jobs: bool = True, targets: set[str] | None = None
) -> IntentResult:
    """Atomically try to set intent.

    Returns 'ok', 'locked', 'invalid', 'unavailable' (Postgres could not be
    reached) or 'error' (Postgres refused the write, and ``detail`` names why).

    *pause_long_jobs* asks Plan 131's pack and prune jobs to stop at their next
    safe boundary (see ``shared.deploy_intent``). It defaults to true because
    the safe behaviour should be the one you get by forgetting; pass false for
    a deploy that touches nothing those jobs depend on.
    """

    if targets is None:
        expanded_targets = frozenset(LEGACY_DEPLOY_TARGETS)
        scope = frozenset(LEGACY_DEPLOY_SCOPE)
    else:
        try:
            expanded_targets, scope = expand_targets(targets)
        except ValueError:
            return IntentResult("invalid")

    params = (caller, pause_long_jobs, STALE_LOCK_MINUTES)

    try:
        with db_cursor(error_context="Set-Intent") as cur:
            cur.execute(ACQUIRE_COORDINATION_LOCK, (COORDINATION_LOCK_ID,))
            cur.execute(SELECT_COORDINATION_STATE_FOR_DEPLOY)
            coordination_row = cur.fetchone()
            if coordination_row is None:
                return IntentResult("error", "coordination_state has no row id=1")
            if coordination_row[1] != "none":
                logger.warning("Deploy intent conflicts with active coordination.")
                return IntentResult("locked")
            cur.execute(SET_DEPLOY_INTENT, params)
            if cur.fetchone() is not None:
                # Compatibility rollout: legacy sensors keep reading
                # deploy_intent while new consumers move to this record.
                cur.execute(
                    REQUEST_DEPLOY_COORDINATION,
                    (
                        json.dumps(sorted(expanded_targets)),
                        json.dumps(sorted(scope)),
                        caller,
                    ),
                )
                changed = cur.fetchone()
                if changed is None:
                    return IntentResult(
                        "error", "coordination_state was not idle when the request was written"
                    )
                generation = changed[0]
                record_transition_event(
                    cur,
                    generation=generation,
                    prior_phase="none",
                    phase="requested",
                    kind="deploy",
                    actor=caller,
                )
            else:
                logger.warning("Intent failed to set — already locked.")
                return IntentResult("locked")
    except Exception as exc:
        # db_cursor has already logged this with its traceback. All that is
        # left is telling the two apart for the caller.
        if isinstance(exc, UNREACHABLE_ERRORS):
            return IntentResult("unavailable")
        return IntentResult("error", db_failure_cause(exc))
    log_transition(
        generation=generation,
        prior_phase="none",
        phase="requested",
        kind="deploy",
    )
    return IntentResult("ok")


def _intent_release() -> IntentResult:
    """Release only a legacy-facade deploy, from any lifecycle phase.

    ``redeploy.sh`` now drives drain, authorization and validation through the
    coordination API. Its existing health gate is the compatibility release
    evidence until Stage 3 exposes the guarded native complete operation.
    Other coordination kinds can never be released through this facade.

    Returns the same statuses ``_set_intent`` does, minus 'invalid'. It returned
    a bare ``bool`` until Plan 162 Stage 6c, collapsing five outcomes into
    ``False`` -- and one of the five is not a failure at all: refusing to
    release another kind's coordination is this facade working, and 'locked'
    is the word the request path already uses for it.
    """
    try:
        with db_cursor(error_context="Intent-Release") as cur:
            cur.execute(ACQUIRE_COORDINATION_LOCK, (COORDINATION_LOCK_ID,))
            cur.execute(SELECT_COORDINATION_STATE_FOR_DEPLOY)
            row = cur.fetchone()
            if row is None:
                return IntentResult("error", "coordination_state has no row id=1")
            if row[1] != "none" and row[0] != "deploy":
                logger.warning("Legacy release refused: %s coordination is active.", row[0])
                return IntentResult(
                    "locked",
                    f"{row[0]} coordination holds the record in phase '{row[1]}'",
                )
            cur.execute(CLEAR_DEPLOY_INTENT)
            if cur.fetchone() is None:
                return IntentResult("error", "deploy_intent has no row id=1")
            if row[0] == "deploy":
                prior_phase = row[1]
                actor = row[3]
                cur.execute(RELEASE_DEPLOY_COORDINATION)
                changed = cur.fetchone()
                if changed is None:
                    return IntentResult(
                        "error",
                        "coordination_state changed while the release was being written",
                    )
                generation = changed[0]
                record_transition_event(
                    cur,
                    generation=generation,
                    prior_phase=prior_phase,
                    phase="none",
                    kind="deploy",
                    actor=actor,
                )
        if row[0] == "deploy":
            log_transition(
                generation=generation,
                prior_phase=prior_phase,
                phase="none",
                kind="deploy",
            )
        return IntentResult("ok")
    except Exception as exc:
        if isinstance(exc, UNREACHABLE_ERRORS):
            return IntentResult("unavailable")
        return IntentResult("error", db_failure_cause(exc))


@router.get("/deploy/status")
def get_current_intent() -> Dict[str, Any]:
    """Returns current intent status and count of running executions."""
    return _intent_status()


@router.post("/deploy/start")
def start_deploy_intent(payload: dict = Body(default={})) -> bool:
    """Signals deploy intent to the system.

    Optional ``pause_long_jobs`` (default **true**) asks Plan 131's pack and
    prune jobs to stop at their next safe boundary and resume after the deploy.
    """
    pause_long_jobs = bool((payload or {}).get("pause_long_jobs", True))
    raw_targets = (payload or {}).get("targets")
    if raw_targets is not None and (
        not isinstance(raw_targets, list)
        or not raw_targets
        or not all(isinstance(target, str) for target in raw_targets)
        or len(set(raw_targets)) != len(raw_targets)
    ):
        raise HTTPException(status_code=422, detail="Invalid deploy targets.")
    targets = None if raw_targets is None else set(raw_targets)
    result = _set_intent("Deploy Declared", pause_long_jobs, targets)
    if result.status == "ok":
        return True
    elif result.status == "locked":
        raise HTTPException(status_code=409, detail="Deploy intent already set.")
    elif result.status == "invalid":
        raise HTTPException(status_code=422, detail="Invalid deploy targets.")
    elif result.status == "unavailable":
        raise HTTPException(status_code=503, detail="Database unavailable.")
    else:
        raise HTTPException(
            status_code=500, detail=f"Deploy intent could not be recorded: {result.detail}"
        )


@router.post("/deploy/complete")
def complete_deployment() -> bool:
    """Releases the intent lock on the DB.

    A failure here is the quiet one. ``redeploy.sh`` calls this from its exit
    trap, so a deploy that succeeded and then failed to release still exits 0
    and sends no alert, leaving every gated DAG parked. Whatever this returns is
    the operator's only account of that, which is why it is no longer one word.
    """
    result = _intent_release()
    if result.status == "ok":
        return True
    elif result.status == "locked":
        raise HTTPException(
            status_code=409, detail=f"Deploy intent cannot be released: {result.detail}"
        )
    elif result.status == "unavailable":
        raise HTTPException(status_code=503, detail="Database unavailable.")
    else:
        raise HTTPException(
            status_code=500, detail=f"Deploy intent could not be released: {result.detail}"
        )
