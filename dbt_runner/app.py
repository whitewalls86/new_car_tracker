from __future__ import annotations

import re
import shlex
import subprocess
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Body, HTTPException

app = FastAPI()

# Update these to match your dbt model names (you already confirmed the detail stg_ targets)
INTENT_TO_SELECT: Dict[str, List[str]] = {
    "after_srp": ["stg_srp_observations+"],
    "after_detail": ["stg_detail_observations+", "stg_detail_carousel_hints+"],
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


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True}


@app.post("/dbt/build")
def dbt_build(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """
    Payload options:
      - intent: required unless select is provided
      - select: optional override (list of selector tokens)
      - exclude: optional list of exclude selector tokens
      - full_refresh: bool
      - fail_fast: bool (default True)
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

    cmd: List[str] = ["dbt", "build"]
    if fail_fast:
        cmd.append("--fail-fast")
    if full_refresh:
        cmd.append("--full-refresh")

    cmd += ["--select", *select]
    if exclude:
        cmd += ["--exclude", *exclude]

    proc = subprocess.run(cmd, capture_output=True, text=True)

    result = {
        "ok": proc.returncode == 0,
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
