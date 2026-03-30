from fastapi import FastAPI, Body, HTTPException
from typing import Any, Dict, List, Optional
import psycopg2
import logging
import os
from logging.handlers import RotatingFileHandler


app = FastAPI()
_LOG_PATH = "/usr/app/logs/app.log"
os.makedirs(os.path.dirname(_LOG_PATH), exist_ok=True)
_log_handler = RotatingFileHandler(_LOG_PATH, maxBytes=5_000_000, backupCount=3)
_log_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
logging.getLogger().addHandler(_log_handler)
logging.getLogger().setLevel(logging.INFO)

logger = logging.getLogger("pipeline_ops")

DB_KWARGS = {
    "host": "postgres",
    "dbname": "cartracker",
    "user": "cartracker",
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

STALE_LOCK_MINUTES = 30


def _intent_status() -> Dict[str, Any]:
    """Return current lock state."""
    try:
        conn = psycopg2.connect(**DB_KWARGS)
        with conn, conn.cursor() as cur:
            cur.execute("""
                            With current_executions AS (
                                SELECT
                                    COUNT(execution_id) as number_running,
                                    MIN(started_at) as min_started_at
                                FROM n8n_executions 
                                WHERE status = 'running'
                            ), current_runs AS (
                                SELECT
                                    COUNT(*) as number_running,
                                    MIN(started_at) as min_started_at
                                FROM runs
                                WHERE status = 'running'
                            ), current_processing_runs AS (
                                SELECT
                                    COUNT(*) as number_running
                                    ,MIN(started_at) as min_started_at
                                FROM  processing_runs
                                WHERE status = 'processing'
                            )
                            SELECT
                                di.intent, 
                                di.requested_at, 
                                di.requested_by, 
                                ce.number_running + cr.number_running + cpr.number_running as number_running,
                                LEAST(ce.min_started_at, cr.min_started_at, cpr.min_started_at) as min_started_at
                            FROM deploy_intent di 
                            LEFT JOIN current_executions ce ON 1=1
                            LEFT JOIN current_runs cr on 1=1
                            LEFT JOIN current_processing_runs cpr ON 1=1
                            WHERE di.id = 1;
                        """)
            row = cur.fetchone()
        conn.close()
        if row:
            return {
                "intent": row[0],
                "requested_at": row[1].isoformat() if row[1] else None,
                "requested_by": row[2],
                "number_running": row[3],
                "min_started_at": row[4].isoformat() if row[4] else None,
            }
        return {"intent": "none", "requested_at": None, "requested_by": None}
    except Exception:
        logger.exception("Failed to read deploy_intent status")
        return {"intent": "none", "requested_at": None, "requested_by": None}


def _set_intent(caller: str) -> bool:
    """Atomically try to set intent. Returns True if set, false if intent already set"""
    try:
        conn = psycopg2.connect(**DB_KWARGS)
        with conn, conn.cursor() as cur:
            cur.execute(
                """UPDATE deploy_intent
                   SET 
                        intent = 'pending', 
                        requested_at = now(), 
                        requested_by = %s
                   WHERE id = 1
                     AND (intent = 'none'
                          OR requested_at < now() - interval '%s minutes')
                   RETURNING intent;""",
                (caller, STALE_LOCK_MINUTES),
            )
            acquired = cur.fetchone() is not None
        conn.close()
        return acquired
    except Exception:
        logger.exception("Failed to set deploy intent")
        return False


def _intent_release() -> bool:
    """Release the intent lock on the DB"""
    try:
        conn = psycopg2.connect(**DB_KWARGS)
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                    UPDATE deploy_intent
                    SET 
                        intent = 'none',
                        requested_at = NULL,
                        requested_by = NULL
                    WHERE id = 1
                    RETURNING intent;
                """
            )
            released = cur.fetchone() is not None
        conn.close()
        return released
    except Exception:
        logger.exception("Failed to set deploy intent release")
        return False


@app.get("/deploy/status")
def get_current_intent() -> Dict[str, Any]:
    """Returns current intent status and count of running executions"""
    return _intent_status()


@app.post("/deploy/start")
def start_deploy_intent() -> bool:
    """ Signals Deploy Intent to the system"""

    return _set_intent("Deploy Declared")


@app.post("/deploy/complete")
def complete_deployment() -> bool:
    """ Releases the intent lock on the DB"""

    return _intent_release()

