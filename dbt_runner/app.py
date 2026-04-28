from __future__ import annotations

import logging
import os
import re
import shlex
import subprocess
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List

from fastapi import Body, FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles

from shared.job_counter import active_job, is_idle

app = FastAPI()
_LOG_PATH = os.getenv("LOG_PATH", "/usr/app/logs/app.log")
os.makedirs(os.path.dirname(_LOG_PATH), exist_ok=True)
_log_handler = RotatingFileHandler(_LOG_PATH, maxBytes=5_000_000, backupCount=3)
_log_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
logging.getLogger().addHandler(_log_handler)
logging.getLogger().setLevel(logging.INFO)

logger = logging.getLogger("dbt_runner")

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


@app.get("/ready")
def ready() -> Dict[str, Any]:
    if is_idle():
        return {"ready": True}
    return {"ready": False, "reason": "jobs in flight"}


@app.get("/logs")
def get_logs(lines: int = 200) -> Dict[str, Any]:
    """Return the last N lines of the application log file."""
    try:
        with open(_LOG_PATH) as f:
            all_lines = f.readlines()
        return {"lines": all_lines[-lines:]}
    except FileNotFoundError:
        return {"lines": []}


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

        logger.info("Running: %s", " ".join(shlex.quote(x) for x in cmd))
        proc = subprocess.run(cmd, capture_output=True, text=True)

        result = {
            "ok": proc.returncode == 0,
            "returncode": proc.returncode,
            "select": select or "all",
            "exclude": exclude or [],
            "cmd": " ".join(shlex.quote(x) for x in cmd),
            "stdout": _cap(proc.stdout),
            "stderr": _cap(proc.stderr),
        }

        if proc.returncode != 0:
            logger.error("dbt build failed (rc=%d)", proc.returncode)
            raise HTTPException(status_code=500, detail=result)

        return result


# ---------------------------------------------------------------------------
# dbt docs static file serving
# ---------------------------------------------------------------------------
_TARGET_DIR = os.path.join(os.getcwd(), "target")
os.makedirs(_TARGET_DIR, exist_ok=True)
app.mount("/dbt-docs", StaticFiles(directory=_TARGET_DIR, html=True), name="dbt_docs")
