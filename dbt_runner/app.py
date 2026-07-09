from __future__ import annotations

import json
import logging
import os
import re
import shlex
import subprocess
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

from fastapi import Body, FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles

from shared.job_counter import active_job, is_idle
from shared.logging_setup import configure_logging

configure_logging()
app = FastAPI()
logger = logging.getLogger("dbt_runner")

SAFE_TOKEN = re.compile(r"^[A-Za-z0-9_:+.@/-]+$")

# Must match the duckdb target in dbt/profiles.yml (Plan 123 Phase 0).
DUCKDB_THREADS = 2
DUCKDB_MEMORY_LIMIT = "8GB"

# Linux SIGKILL exit codes: -9 from a direct signal, 137 (128+9) from some
# shells/container runtimes that report it as a plain exit status.
_OOM_RETURNCODES = (-9, 137)


def _likely_oom(returncode: int) -> bool:
    return returncode in _OOM_RETURNCODES


def _model_timings_from_run_results() -> List[Dict[str, Any]]:
    """Best-effort per-model timing from the run_results.json dbt just wrote."""
    path = os.path.join(os.getcwd(), "target", "run_results.json")
    if not os.path.exists(path):
        return []
    try:
        with open(path) as f:
            data = json.load(f)
    except (OSError, ValueError):
        return []
    return [
        {
            "unique_id": result.get("unique_id"),
            "status": result.get("status"),
            "execution_time": result.get("execution_time"),
        }
        for result in data.get("results", [])
    ]


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


@app.get("/ready")
def ready() -> Dict[str, Any]:
    if is_idle():
        return {"ready": True}
    raise HTTPException(status_code=503, detail={"ready": False, "reason": "jobs in flight"})


@app.get("/dbt/docs/status")
def get_docs_status() -> Dict[str, Any]:
    """Check whether dbt docs have been generated (target/index.html exists)."""
    available = os.path.exists(os.path.join(os.getcwd(), "target", "index.html"))
    return {"available": available}


@app.post("/dbt/docs/generate")
def dbt_docs_generate() -> Dict[str, Any]:
    """Run dbt deps + dbt docs generate and return ok/stdout/stderr."""
    deps = subprocess.run(["dbt", "deps"], capture_output=True, text=True)
    if deps.returncode != 0:
        logger.error("dbt deps failed (rc=%d): %s", deps.returncode, deps.stderr)
        raise HTTPException(status_code=500, detail={
            "ok": False,
            "returncode": deps.returncode,
            "stdout": _cap(deps.stdout),
            "stderr": _cap(deps.stderr),
        })

    proc = subprocess.run(["dbt", "docs", "generate"], capture_output=True, text=True)
    ok = proc.returncode == 0
    if not ok:
        logger.error("dbt docs generate failed (rc=%d): %s", proc.returncode, proc.stderr)
        raise HTTPException(status_code=500, detail={
            "ok": False,
            "returncode": proc.returncode,
            "stdout": _cap(deps.stdout + proc.stdout),
            "stderr": _cap(proc.stderr),
        })

    return {
        "ok": True,
        "returncode": 0,
        "stdout": _cap(deps.stdout + proc.stdout),
        "stderr": "",
    }


@app.post("/dbt/build")
def dbt_build(payload: Dict[str, Any] = Body(default={})) -> Dict[str, Any]:
    """
    Trigger a dbt build against the DuckDB analytics target.

    Payload options (all optional):
      - select:       list of dbt selector tokens; omit to build all models
      - exclude:      list of selector tokens to exclude
      - full_refresh: bool (default False)
      - fail_fast:    bool (default True)

    Returns 409 if a build is already in progress.
    Returns 500 if dbt exits non-zero.
    """
    if not is_idle():
        raise HTTPException(
            status_code=409,
            detail={"error": "dbt_build_in_progress", "message": "A dbt build is already running."},
        )

    with active_job():

        full_refresh: bool = bool(payload.get("full_refresh", False))
        fail_fast: bool = bool(payload.get("fail_fast", True))

        select = payload.get("select")
        exclude = payload.get("exclude")

        if isinstance(select, str):
            select = [select]
        if isinstance(exclude, str):
            exclude = [exclude]

        if select:
            _validate_tokens(select, "select")
        if exclude:
            _validate_tokens(exclude, "exclude")

        cmd: List[str] = ["dbt", "build", "--target", "duckdb"]
        if fail_fast:
            cmd.append("--fail-fast")
        if full_refresh:
            cmd.append("--full-refresh")
        if select:
            cmd += ["--select", *select]
        if exclude:
            cmd += ["--exclude", *exclude]

        invocation_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc).isoformat()
        cmd_str = " ".join(shlex.quote(x) for x in cmd)
        logger.info("dbt build invocation=%s starting: %s", invocation_id, cmd_str)

        start = time.monotonic()
        proc = subprocess.run(cmd, capture_output=True, text=True)
        duration_seconds = round(time.monotonic() - start, 2)
        ended_at = datetime.now(timezone.utc).isoformat()
        likely_oom = _likely_oom(proc.returncode)

        result = {
            "ok": proc.returncode == 0,
            "returncode": proc.returncode,
            "likely_oom": likely_oom,
            "invocation_id": invocation_id,
            "started_at": started_at,
            "ended_at": ended_at,
            "duration_seconds": duration_seconds,
            "select": select or "all",
            "exclude": exclude or [],
            "full_refresh": full_refresh,
            "cmd": cmd_str,
            "duckdb_threads": DUCKDB_THREADS,
            "duckdb_memory_limit": DUCKDB_MEMORY_LIMIT,
            "model_timings": _model_timings_from_run_results(),
            "stdout": _cap(proc.stdout),
            "stderr": _cap(proc.stderr),
        }

        logger.info(
            "dbt build invocation=%s rc=%d duration=%.2fs full_refresh=%s likely_oom=%s",
            invocation_id, proc.returncode, duration_seconds, full_refresh, likely_oom,
        )

        if proc.returncode != 0:
            logger.error(
                "dbt build failed invocation=%s (rc=%d)\nstdout: %s\nstderr: %s",
                invocation_id, proc.returncode, proc.stdout, proc.stderr,
            )
            raise HTTPException(status_code=500, detail=result)

        return result


# ---------------------------------------------------------------------------
# dbt docs static file serving
# ---------------------------------------------------------------------------
_TARGET_DIR = os.path.join(os.getcwd(), "target")
os.makedirs(_TARGET_DIR, exist_ok=True)
app.mount("/dbt-docs", StaticFiles(directory=_TARGET_DIR, html=True), name="dbt_docs")
