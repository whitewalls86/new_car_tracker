"""
Deploy coordination API endpoints.
"""
import logging
from typing import Any, Dict

from fastapi import APIRouter, HTTPException

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
            LEAST(pa.min_started_at, rdc.min_started_at) AS min_started_at
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
            }
        else:
            results = {"intent": "none", "requested_at": None, "requested_by": None}
    except Exception:
        results = {"intent": "none", "requested_at": None, "requested_by": None}

    return results


def _set_intent(caller: str) -> str:
    """Atomically try to set intent. Returns 'ok', 'locked', or 'error'."""

    sql = """UPDATE deploy_intent
                   SET
                        intent = 'pending',
                        requested_at = now(),
                        requested_by = %s
                   WHERE id = 1
                     AND (intent = 'none'
                          OR requested_at < now() - interval '%s minutes')
                   RETURNING intent;"""
    params = (caller, STALE_LOCK_MINUTES)

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
def start_deploy_intent() -> bool:
    """Signals deploy intent to the system."""
    result = _set_intent("Deploy Declared")
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
    