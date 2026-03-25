from __future__ import annotations

import os
import re
import shlex
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import psycopg2
from fastapi import FastAPI, Body, HTTPException

app = FastAPI()

DB_KWARGS = {
    "host": "postgres",
    "dbname": "cartracker",
    "user": "cartracker",
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}
_MODEL_COUNTS_RE = re.compile(r"PASS=(\d+)\s+WARN=\d+\s+ERROR=(\d+)\s+SKIP=(\d+)")

# Stale lock timeout in minutes — if a lock is held longer than this,
# it's assumed the holder crashed and the lock can be stolen.
STALE_LOCK_MINUTES = 30


# ---------------------------------------------------------------------------
# Lock helpers
# ---------------------------------------------------------------------------

def _acquire_lock(caller: str) -> bool:
    """Atomically try to acquire the dbt_lock. Returns True if acquired."""
    try:
        conn = psycopg2.connect(**DB_KWARGS)
        with conn, conn.cursor() as cur:
            cur.execute(
                """UPDATE dbt_lock
                   SET locked = true, locked_at = now(), locked_by = %s
                   WHERE id = 1
                     AND (locked = false
                          OR locked_at < now() - interval '%s minutes')
                   RETURNING locked;""",
                (caller, STALE_LOCK_MINUTES),
            )
            acquired = cur.fetchone() is not None
        conn.close()
        return acquired
    except Exception:
        return False


def _release_lock() -> None:
    """Release the dbt_lock."""
    try:
        conn = psycopg2.connect(**DB_KWARGS)
        with conn, conn.cursor() as cur:
            cur.execute(
                """UPDATE dbt_lock
                   SET locked = false, locked_at = null, locked_by = null
                   WHERE id = 1;"""
            )
        conn.close()
    except Exception:
        pass


def _lock_status() -> Dict[str, Any]:
    """Return current lock state."""
    try:
        conn = psycopg2.connect(**DB_KWARGS)
        with conn, conn.cursor() as cur:
            cur.execute("SELECT locked, locked_at, locked_by FROM dbt_lock WHERE id = 1;")
            row = cur.fetchone()
        conn.close()
        if row:
            return {
                "locked": row[0],
                "locked_at": row[1].isoformat() if row[1] else None,
                "locked_by": row[2],
            }
        return {"locked": False, "locked_at": None, "locked_by": None}
    except Exception:
        return {"locked": False, "locked_at": None, "locked_by": None}


# ---------------------------------------------------------------------------
# Run logging
# ---------------------------------------------------------------------------

def _record_run(started_at: datetime, finished_at: datetime, ok: bool,
                intent: Optional[str], select: List[str],
                stdout: str, returncode: int) -> None:
    m = _MODEL_COUNTS_RE.search(stdout)
    models_pass = int(m.group(1)) if m else None
    models_error = int(m.group(2)) if m else None
    models_skip = int(m.group(3)) if m else None
    duration_s = (finished_at - started_at).total_seconds()
    try:
        conn = psycopg2.connect(**DB_KWARGS)
        with conn, conn.cursor() as cur:
            cur.execute(
                """INSERT INTO dbt_runs
                   (started_at, finished_at, duration_s, ok, intent, select_args,
                    models_pass, models_error, models_skip, returncode)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (started_at, finished_at, duration_s, ok, intent,
                 " ".join(select), models_pass, models_error, models_skip, returncode),
            )
        conn.close()
    except Exception:
        pass  # never let DB logging break the build response


# Update these to match your dbt model names (you already confirmed the detail stg_ targets)
INTENT_TO_SELECT: Dict[str, List[str]] = {
    "after_srp": ["stg_srp_observations+"],
    "after_detail": ["stg_detail_observations+", "stg_detail_carousel_hints+", "ops_vehicle_staleness+"],
}

SAFE_TOKEN = re.compile(r"^[A-Za-z0-9_:+.@/-]+$")


def _validate_tokens(tokens: List[str], field: str) -> None:
    for t in tokens:
        if not t or not SAFE_TOKEN.match(t):
            raise HTTPException(status_code=400, detail=f"Invalid {field} token: {t!r}")


def _cap(s: str, limit: int = 20000) -> str:
    if s is None:
        return ""
    return s if len(s) <= limit else s[-limit:]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True}


@app.get("/dbt/lock")
def get_lock_status() -> Dict[str, Any]:
    """Return the current lock state. Used by the dashboard."""
    return _lock_status()


@app.post("/dbt/build")
def dbt_build(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """
    Payload options:
      - intent: required unless select is provided
      - select: optional override (list of selector tokens)
      - exclude: optional list of exclude selector tokens
      - full_refresh: bool
      - fail_fast: bool (default True)

    The lock is acquired before the build and released after,
    regardless of success or failure. If the lock is already held,
    returns 409 Conflict so the caller can retry.
    """
    intent: Optional[str] = payload.get("intent")
    full_refresh: bool = bool(payload.get("full_refresh", False))
    fail_fast: bool = bool(payload.get("fail_fast", True))

    select = payload.get("select")
    exclude = payload.get("exclude")

    if select is None:
        if not intent:
            raise HTTPException(status_code=400, detail="Provide either 'intent' or 'select'.")
        if intent not in INTENT_TO_SELECT:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown intent {intent!r}. Allowed: {sorted(INTENT_TO_SELECT.keys())}",
            )
        select = INTENT_TO_SELECT[intent]

    if isinstance(select, str):
        select = [select]
    if exclude is not None and isinstance(exclude, str):
        exclude = [exclude]

    _validate_tokens(select, "select")
    if exclude:
        _validate_tokens(exclude, "exclude")

    # --- Acquire lock ---
    caller = intent or "manual"
    if not _acquire_lock(caller):
        status = _lock_status()
        raise HTTPException(
            status_code=409,
            detail={
                "error": "dbt_locked",
                "message": f"dbt build already in progress (locked by: {status.get('locked_by')})",
                "lock": status,
            },
        )

    try:
        cmd: List[str] = ["dbt", "build"]
        if fail_fast:
            cmd.append("--fail-fast")
        if full_refresh:
            cmd.append("--full-refresh")

        cmd += ["--select", *select]
        if exclude:
            cmd += ["--exclude", *exclude]

        started_at = datetime.now(timezone.utc)
        proc = subprocess.run(cmd, capture_output=True, text=True)
        finished_at = datetime.now(timezone.utc)

        ok = proc.returncode == 0
        _record_run(started_at, finished_at, ok, intent, select, proc.stdout, proc.returncode)

        result = {
            "ok": ok,
            "returncode": proc.returncode,
            "intent": intent,
            "select": select,
            "exclude": exclude or [],
            "cmd": " ".join(shlex.quote(x) for x in cmd),
            "stdout": _cap(proc.stdout),
            "stderr": _cap(proc.stderr),
        }

        # HARD FAIL: make n8n fail via non-2xx
        if proc.returncode != 0:
            raise HTTPException(status_code=500, detail=result)

        return result

    finally:
        # --- Always release lock ---
        _release_lock()
