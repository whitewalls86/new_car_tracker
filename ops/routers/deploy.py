"""
Deploy coordination API endpoints.
"""

import json
import logging
from typing import Any, Dict

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
from shared.db import db_cursor

logger = logging.getLogger("pipeline_ops")
router = APIRouter()

STALE_LOCK_MINUTES = 30
COORDINATION_LOCK_ID = 142
LEGACY_DEPLOY_TARGETS = tuple(sorted(SERVICE_CONTRACTS))
LEGACY_DEPLOY_SCOPE = tuple(sorted(SURFACES - {"host"}))


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


def _set_intent(caller: str, pause_long_jobs: bool = True, targets: set[str] | None = None) -> str:
    """Atomically try to set intent. Returns 'ok', 'locked', or 'error'.

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
            return "invalid"

    params = (caller, pause_long_jobs, STALE_LOCK_MINUTES)

    try:
        with db_cursor(error_context="Set-Intent") as cur:
            cur.execute(ACQUIRE_COORDINATION_LOCK, (COORDINATION_LOCK_ID,))
            cur.execute(SELECT_COORDINATION_STATE_FOR_DEPLOY)
            coordination_row = cur.fetchone()
            if coordination_row is None:
                return "error"
            if coordination_row[1] != "none":
                logger.warning("Deploy intent conflicts with active coordination.")
                return "locked"
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
                    return "error"
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
                return "locked"
    except Exception:
        return "error"
    log_transition(
        generation=generation,
        prior_phase="none",
        phase="requested",
        kind="deploy",
    )
    return "ok"


def _intent_release() -> bool:
    """Release only a legacy-facade deploy, from any lifecycle phase.

    ``redeploy.sh`` now drives drain, authorization and validation through the
    coordination API. Its existing health gate is the compatibility release
    evidence until Stage 3 exposes the guarded native complete operation.
    Other coordination kinds can never be released through this facade.
    """
    try:
        with db_cursor(error_context="Intent-Release") as cur:
            cur.execute(ACQUIRE_COORDINATION_LOCK, (COORDINATION_LOCK_ID,))
            cur.execute(SELECT_COORDINATION_STATE_FOR_DEPLOY)
            row = cur.fetchone()
            if row is None:
                return False
            if row[1] != "none" and row[0] != "deploy":
                return False
            cur.execute(CLEAR_DEPLOY_INTENT)
            if cur.fetchone() is None:
                return False
            if row[0] == "deploy":
                prior_phase = row[1]
                actor = row[3]
                cur.execute(RELEASE_DEPLOY_COORDINATION)
                changed = cur.fetchone()
                if changed is None:
                    return False
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
        return True
    except Exception:
        return False


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
    if result == "ok":
        return True
    elif result == "locked":
        raise HTTPException(status_code=409, detail="Deploy intent already set.")
    elif result == "invalid":
        raise HTTPException(status_code=422, detail="Invalid deploy targets.")
    else:
        raise HTTPException(status_code=503, detail="Database unavailable.")


@router.post("/deploy/complete")
def complete_deployment() -> bool:
    """Releases the intent lock on the DB."""
    result = _intent_release()
    if result:
        return result
    else:
        raise HTTPException(status_code=503, detail="Database unavailable.")
