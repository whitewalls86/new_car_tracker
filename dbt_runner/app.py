from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
import re
import shlex
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Literal, overload

import psycopg2
from fastapi import FastAPI, Body, HTTPException
from fastapi.staticfiles import StaticFiles
from enum import Enum
import json


class FetchMode(Enum):
    NONE = None       # No fetch
    ONE = "one"       # fetchone()
    ALL = "all"       # fetchall()
    ROWCOUNT = "rowcount"  # cur.rowcount

app = FastAPI()
_LOG_PATH = "/usr/app/logs/app.log"
os.makedirs(os.path.dirname(_LOG_PATH), exist_ok=True)
_log_handler = RotatingFileHandler(_LOG_PATH, maxBytes=5_000_000, backupCount=3)
_log_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
logging.getLogger().addHandler(_log_handler)
logging.getLogger().setLevel(logging.INFO)

logger = logging.getLogger("dbt_runner")

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
# DB helpers
# ---------------------------------------------------------------------------

@overload
def _db_execute(sql: str, params: tuple = None, fetch: Literal[FetchMode.NONE] = FetchMode.NONE,
                error_context: str = 'DB Operation') -> bool | None: ...


@overload
def _db_execute(sql: str, params: tuple = None, fetch: Literal[FetchMode.ONE] = FetchMode.NONE,
                error_context: str = 'DB Operation') -> tuple | None: ...


@overload
def _db_execute(sql: str, params: tuple = None, fetch: Literal[FetchMode.ALL] = FetchMode.NONE,
                error_context: str = 'DB Operation') -> list | None: ...


@overload
def _db_execute(sql: str, params: tuple = None, fetch: Literal[FetchMode.ROWCOUNT] = FetchMode.NONE,
                error_context: str = 'DB Operation') -> int | None: ...


def _db_execute(sql: str, params: tuple = None, fetch: FetchMode = FetchMode.NONE,
                error_context: str = 'DB Operation'):
    conn = None
    try:
        conn = psycopg2.connect(**DB_KWARGS)
    except psycopg2.OperationalError:
        msg = f"{error_context}: Unable to connect to Postgres database."
        logger.error(msg)
        return
    except Exception:
        msg = f"{error_context}: encountered DB error."
        logger.error(msg)
        return

    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, params or ())
            if fetch == FetchMode.ONE:
                return cur.fetchone() or ()
            elif fetch == FetchMode.ALL:
                return cur.fetchall() or []
            elif fetch == FetchMode.ROWCOUNT:
                return cur.rowcount or 0
            elif fetch == FetchMode.NONE:
                return True

    except Exception as e:
        msg = f"{error_context}: SQL execution failed."
        logger.error(msg)
        return
    finally:
        if conn:
            conn.close()


# ---------------------------------------------------------------------------
# Lock helpers
# ---------------------------------------------------------------------------

def _acquire_lock(caller: str) -> bool:
    """Atomically try to acquire the dbt_lock. Returns True if acquired."""
    sql = """
        UPDATE dbt_lock
        SET locked = true, locked_at = now(), locked_by = %s
        WHERE id = 1
            AND (locked = false
            OR locked_at < now() - interval '%s minutes')
       RETURNING locked;
       """
    params = (caller, STALE_LOCK_MINUTES)

    acquired = _db_execute(sql=sql, params=params, fetch=FetchMode.ONE, error_context='Acquire-Lock')

    return bool(acquired)


def _release_lock() -> None:
    """Release the dbt_lock."""

    sql = """UPDATE dbt_lock
                   SET locked = false, locked_at = null, locked_by = null
                   WHERE id = 1;"""
    released = _db_execute(sql=sql, fetch=FetchMode.NONE, error_context='Release-Lock')

    if not released:
        # Raise Error
        return


def _lock_status() -> Dict[str, Any]:
    """Return current lock state."""

    sql = """SELECT locked, locked_at, locked_by FROM dbt_lock WHERE id = 1"""

    status = _db_execute(sql=sql, fetch=FetchMode.ONE, error_context='Lock-Status')

    if status is None:
        return {"locked": True, "locked_at": None, "locked_by": "DB Error"}
    elif status == ():
        return {"locked": False, "locked_at": None, "locked_by": None}
    else:
        return {
            "locked": status[0],
            "locked_at": status[1].isoformat() if status[1] else None,
            "locked_by": status[2],
        }


# ---------------------------------------------------------------------------
# Run logging
# ---------------------------------------------------------------------------

def _record_run(started_at: datetime, finished_at: datetime, ok: bool,
                intent: Optional[str], select: List[str],
                stdout: str, returncode: int) -> bool:
    m = _MODEL_COUNTS_RE.search(stdout)
    models_pass = int(m.group(1)) if m else None
    models_error = int(m.group(2)) if m else None
    models_skip = int(m.group(3)) if m else None
    duration_s = (finished_at - started_at).total_seconds()

    sql = """INSERT INTO dbt_runs
                   (started_at, finished_at, duration_s, ok, intent, select_args,
                    models_pass, models_error, models_skip, returncode)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    params = (started_at, finished_at, duration_s, ok, intent,
              " ".join(select), models_pass, models_error, models_skip, returncode)

    result = _db_execute(sql=sql, params=params, fetch=FetchMode.NONE, error_context='Record-Run')

    if result is not True:
        return False
    else:
        return result

# ---------------------------------------------------------------------------
# Intent management (DB-backed, replaces hardcoded INTENT_TO_SELECT)
# ---------------------------------------------------------------------------

# Fallback used if the dbt_intents table doesn't exist yet (before migration).
_INTENT_FALLBACK: Dict[str, List[str]] = {
    "after_srp": ["stg_raw_artifacts+", "stg_srp_observations+", "stg_detail_carousel_hints+"],
    "after_detail": ["stg_raw_artifacts+", "stg_detail_observations+", "stg_detail_carousel_hints+"],
}


def _load_intents() -> Dict[str, List[str]]:
    """Read intents from dbt_intents table. Falls back to hardcoded defaults."""

    sql = """SELECT intent_name, select_args FROM dbt_intents ORDER BY intent_name"""

    results = _db_execute(sql=sql, fetch=FetchMode.ALL, error_context='Load-Intents')

    if not results:
        logger.warning("Could not load intents from DB, using fallback")
        return dict(_INTENT_FALLBACK)
    else:
        return {result[0]: list(result[1]) for result in results}


def _save_intent(intent_name: str, select_args: List[str]) -> None:
    conn = psycopg2.connect(**DB_KWARGS)
    with conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO dbt_intents (intent_name, select_args, updated_at)
               VALUES (%s, %s, now())
               ON CONFLICT (intent_name) DO UPDATE
               SET select_args = EXCLUDED.select_args, updated_at = now()""",
            (intent_name, select_args),
        )
    conn.close()


def _delete_intent(intent_name: str) -> bool:
    conn = psycopg2.connect(**DB_KWARGS)
    with conn, conn.cursor() as cur:
        cur.execute("DELETE FROM dbt_intents WHERE intent_name = %s", (intent_name,))
        deleted = cur.rowcount > 0
    conn.close()
    return deleted

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


@app.get("/logs")
def get_logs(lines: int = 200) -> Dict[str, Any]:
    """Return the last N lines of the application log file."""
    try:
        with open(_LOG_PATH) as f:
            all_lines = f.readlines()
        return {"lines": all_lines[-lines:]}
    except FileNotFoundError:
        return {"lines": []}


@app.get("/dbt/lock")
def get_lock_status() -> Dict[str, Any]:
    """Return the current lock state. Used by the dashboard."""
    return _lock_status()


@app.get("/dbt/intents")
def get_intents() -> Dict[str, Any]:
    """Return all intents from the DB."""
    intents = _load_intents()
    return {"intents": {k: {"select": v} for k, v in intents.items()}}


@app.post("/dbt/intents")
def upsert_intent(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """Create or update an intent. Payload: {intent_name, select_args: []}"""
    intent_name = (payload.get("intent_name") or "").strip()
    select_args = payload.get("select_args") or []
    if not intent_name:
        raise HTTPException(status_code=400, detail="intent_name is required")
    if isinstance(select_args, str):
        select_args = [t.strip() for t in select_args.split() if t.strip()]
    _validate_tokens(select_args, "select_args")
    _save_intent(intent_name, select_args)
    return {"ok": True, "intent_name": intent_name, "select_args": select_args}


@app.delete("/dbt/intents/{intent_name}")
def delete_intent(intent_name: str) -> Dict[str, Any]:
    """Delete an intent by name."""
    deleted = _delete_intent(intent_name)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Intent {intent_name!r} not found")
    return {"ok": True, "deleted": intent_name}


@app.get("/dbt/docs/status")
def get_docs_status() -> Dict[str, Any]:
    """Check whether dbt docs have been generated (target/index.html exists)."""
    available = os.path.exists(os.path.join(os.getcwd(), "target", "index.html"))
    return {"available": available}


@app.post("/dbt/docs/generate")
def dbt_docs_generate() -> Dict[str, Any]:
    """Run dbt deps + dbt docs generate and return ok/stdout/stderr."""
    # Ensure packages are installed before generating docs.
    deps = subprocess.run(["dbt", "deps"], capture_output=True, text=True)
    if deps.returncode != 0:
        logger.error("dbt deps failed (rc=%d): %s", deps.returncode, deps.stderr)
        return {
            "ok": False,
            "returncode": deps.returncode,
            "stdout": _cap(deps.stdout),
            "stderr": _cap(deps.stderr),
        }

    proc = subprocess.run(["dbt", "docs", "generate"], capture_output=True, text=True)
    ok = proc.returncode == 0
    if not ok:
        logger.error("dbt docs generate failed (rc=%d): %s", proc.returncode, proc.stderr)
    return {
        "ok": ok,
        "returncode": proc.returncode,
        "stdout": _cap(deps.stdout + proc.stdout),
        "stderr": _cap(proc.stderr),
    }


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
        intent_map = _load_intents()
        if intent not in intent_map:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown intent {intent!r}. Allowed: {sorted(intent_map.keys())}",
            )
        select = intent_map[intent]

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
        is_successful = _record_run(started_at, finished_at, ok, intent, select, proc.stdout, proc.returncode)

        if not is_successful:
            data = {
                "started_at": started_at,
                "finished_at": finished_at,
                "ok": ok,
                "intent": intent,
                "select": select,
                "stdout": proc.stdout,
                "returncode": proc.returncode
            }
            logger.error(f"Logging Run Failed. {json.dumps(data)}")

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


# ---------------------------------------------------------------------------
# dbt docs static file serving
# Mount target/ so generated docs are browsable at /docs/
# The directory is created here to ensure the mount doesn't fail on first start
# (before dbt docs generate has been run).
# ---------------------------------------------------------------------------
_TARGET_DIR = os.path.join(os.getcwd(), "target")
os.makedirs(_TARGET_DIR, exist_ok=True)
app.mount("/dbt-docs", StaticFiles(directory=_TARGET_DIR, html=True), name="dbt_docs")
