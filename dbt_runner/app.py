from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
import re
import shlex
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import psycopg2
from fastapi import FastAPI, Body, HTTPException
from fastapi.staticfiles import StaticFiles

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
        logger.exception("Failed to acquire dbt_lock")
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
        logger.exception("Failed to release dbt_lock")


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
        logger.exception("Failed to read dbt_lock status")
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
        logger.exception("Failed to record dbt run to DB")  # never let DB logging break the build response


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
    try:
        conn = psycopg2.connect(**DB_KWARGS)
        with conn, conn.cursor() as cur:
            cur.execute("SELECT intent_name, select_args FROM dbt_intents ORDER BY intent_name")
            rows = cur.fetchall()
        conn.close()
        if rows:
            return {row[0]: list(row[1]) for row in rows}
    except Exception:
        logger.warning("Could not load intents from DB, using fallback", exc_info=True)
    return dict(_INTENT_FALLBACK)


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


# ---------------------------------------------------------------------------
# dbt docs static file serving
# Mount target/ so generated docs are browsable at /docs/
# The directory is created here to ensure the mount doesn't fail on first start
# (before dbt docs generate has been run).
# ---------------------------------------------------------------------------
_TARGET_DIR = os.path.join(os.getcwd(), "target")
os.makedirs(_TARGET_DIR, exist_ok=True)
app.mount("/dbt-docs", StaticFiles(directory=_TARGET_DIR, html=True), name="dbt_docs")
