"""
Deploy coordination API endpoints.
"""
import logging
from typing import Any, Dict

from fastapi import APIRouter, Body, HTTPException

from shared.db import db_cursor

logger = logging.getLogger("pipeline_ops")
router = APIRouter()

STALE_LOCK_MINUTES = 30


def _intent_status() -> Dict[str, Any]:
    """Return current deploy intent state plus in-flight counts."""

    sql = """
        WITH pending_artifacts AS (
            SELECT
                COUNT(*) AS number_running,
                MIN(created_at) AS min_started_at
            FROM ops.artifacts_queue
            WHERE status IN ('pending', 'processing')
        ), running_detail_claims AS (
            SELECT
                COUNT(*) AS number_running,
                MIN(claimed_at) AS min_started_at
            FROM ops.detail_scrape_claims
            WHERE status = 'running'
        )
        SELECT
            di.intent,
            di.requested_at,
            di.requested_by,
            pa.number_running + rdc.number_running AS number_running,
            LEAST(pa.min_started_at, rdc.min_started_at) AS min_started_at,
            di.pause_long_jobs
        FROM deploy_intent di
        LEFT JOIN pending_artifacts pa ON 1=1
        LEFT JOIN running_detail_claims rdc ON 1=1
        WHERE di.id = 1
    """

    try:
        with db_cursor(error_context='Intent-Status') as cur:
            cur.execute(sql)
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


def _set_intent(caller: str, pause_long_jobs: bool = True) -> str:
    """Atomically try to set intent. Returns 'ok', 'locked', or 'error'.

    *pause_long_jobs* asks Plan 131's pack and prune jobs to stop at their next
    safe boundary (see ``shared.deploy_intent``). It defaults to true because
    the safe behaviour should be the one you get by forgetting; pass false for
    a deploy that touches nothing those jobs depend on.
    """

    sql = """UPDATE deploy_intent
                   SET
                        intent = 'pending',
                        requested_at = now(),
                        requested_by = %s,
                        pause_long_jobs = %s
                   WHERE id = 1
                     AND (intent = 'none'
                          OR requested_at < now() - interval '%s minutes')
                   RETURNING intent;"""
    params = (caller, pause_long_jobs, STALE_LOCK_MINUTES)

    try:
        with db_cursor(error_context="Set-Intent") as cur:
            cur.execute(sql, params)
            if cur.fetchone() is not None:
                return "ok"
            logger.warning("Intent failed to set — already locked.")
            return "locked"
    except Exception:
        return "error"


def _intent_release() -> bool:
    """Release the deploy intent lock."""
    sql = """UPDATE deploy_intent
                   SET
                       intent = 'none',
                       requested_at = NULL,
                       requested_by = NULL
                   WHERE id = 1
                   RETURNING intent;"""
    try:
        with db_cursor(error_context="Intent-Release") as cur:
            cur.execute(sql)
            return cur.fetchone() is not None
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
    result = _set_intent("Deploy Declared", pause_long_jobs)
    if result == "ok":
        return True
    elif result == "locked":
        raise HTTPException(status_code=409, detail="Deploy intent already set.")
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
    