"""
Admin UI routes — migrated from scraper container.
All routes use sync psycopg2 (FastAPI threadpools them automatically).
"""
import json
import os
import re
from datetime import UTC, datetime
from typing import Optional

import requests as http_requests
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from shared.db import db_cursor

from ..models.search_config import SORT_KEYS, SORT_OPTIONS, SearchConfigParams
from ..routers.deploy import _intent_release, _intent_status, _set_intent

DBT_RUNNER_URL = os.environ.get("DBT_RUNNER_URL", "http://dbt_runner:8080")
DBT_DOCS_URL = os.environ.get("DBT_DOCS_URL", "http://localhost:8081/dbt-docs/")
SCRAPER_URL = os.environ.get("SCRAPER_URL", "http://scraper:8000")

router = APIRouter()
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _slug(text: str) -> str:
    """Convert text to a URL-safe slug for search_key."""
    return re.sub(r"[^a-z0-9_-]", "", text.lower().replace(" ", "-").replace("/", "-"))


def _parse_comma_list(raw: str) -> list[str]:
    """Split comma-separated string into a list of trimmed, non-empty strings."""
    return [s.strip() for s in raw.split(",") if s.strip()]


def _row_to_dict(row: dict) -> dict:
    """Ensure params JSON is unpacked if it's still a string."""
    d = dict(row)
    if isinstance(d.get("params"), str):
        d["params"] = json.loads(d["params"])
    return d


def _stringify_uuids(d: dict) -> dict:
    """Convert any UUID values to plain strings so templates can slice/compare them."""
    return {k: str(v) if hasattr(v, 'hex') and hasattr(v, 'bytes') else v for k, v in d.items()}


def _db_error_response(request: Request):
    return templates.TemplateResponse(request=request, name="admin/error.html", context={
        "request": request,
        "message": "Database unavailable. Please try again later.",
    }, status_code=503)

# ---------------------------------------------------------------------------
# Search config list
# ---------------------------------------------------------------------------

@router.get("/searches/", response_class=HTMLResponse)
def list_searches(request: Request):
    sql = """SELECT 
                search_key, 
                enabled, 
                source, 
                params, 
                rotation_order, 
                last_queued_at, 
                created_at, 
                updated_at
            FROM search_configs ORDER BY enabled DESC, rotation_order NULLS LAST, search_key"""
    
    try:
        with db_cursor(error_context="List-Searches", dict_cursor=True) as cur:
            cur.execute(sql)
            rows = cur.fetchall()

    except Exception:
        return _db_error_response(request=request)
    
    configs = [_row_to_dict(r) for r in rows]

    return templates.TemplateResponse(request=request, name="admin/list.html", context={
        "request": request,
        "configs": configs,
    })


# ---------------------------------------------------------------------------
# New config form
# ---------------------------------------------------------------------------

@router.get("/searches/new", response_class=HTMLResponse)
def new_search_form(request: Request):
    return templates.TemplateResponse(request=request, name="admin/form.html", context={
        "request": request,
        "editing": False,
        "config": None,
        "sort_options": SORT_OPTIONS,
        "error": None,
    })


# ---------------------------------------------------------------------------
# Run history
# ---------------------------------------------------------------------------

@router.get("/runs", response_class=HTMLResponse)
def list_runs(request: Request):

    sql = """SELECT run_id, started_at, finished_at, status, trigger,
                   progress_count, total_count, error_count, last_error, notes
            FROM runs
            ORDER BY started_at DESC
            LIMIT 20"""
    
    try:
        with db_cursor(error_context="Get-Runs", dict_cursor=True) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception:
        return _db_error_response(request=request)

    runs = [_stringify_uuids(dict(r)) for r in rows]
    return templates.TemplateResponse(request=request, name="admin/runs.html", context={
        "request": request,
        "runs": runs,
    })


@router.get("/runs/{run_id}", response_class=HTMLResponse)
def run_detail(request: Request, run_id: str):
    sql_runs = """
            SELECT run_id, started_at, finished_at, status, trigger,
                   progress_count, total_count, error_count, last_error, notes
            FROM runs WHERE run_id = %s
        """
    sql_jobs = """
            SELECT job_id, search_key, scope, status, created_at,
                   started_at, completed_at, artifact_count, error, retry_count
            FROM scrape_jobs
            WHERE run_id = %s
            ORDER BY created_at
        """
    params = (run_id,)
    run = None
    jobs = []

    try:
        with db_cursor(error_context="Get-Runs", dict_cursor=True) as cur:
            cur.execute(sql_runs, params)
            run = cur.fetchone()

            if not run:
                return RedirectResponse(url="/admin/runs", status_code=303)

            cur.execute(sql_jobs, params)
            jobs = cur.fetchall()
    except Exception:
        return _db_error_response(request=request)

    return templates.TemplateResponse(request=request, name="admin/run_detail.html", context={
        "request": request,
        "run": _stringify_uuids(dict(run)),
        "jobs": [_stringify_uuids(dict(j)) for j in jobs],
    })


# ---------------------------------------------------------------------------
# dbt action panel
# ---------------------------------------------------------------------------

def _fetch_dbt_context() -> dict:
    """Fetch lock status, intents, and docs availability from dbt_runner."""
    lock = {"locked": False, "locked_at": None, "locked_by": None}
    intents = {}
    docs_available = False

    try:
        resp = http_requests.get(f"{DBT_RUNNER_URL}/dbt/lock", timeout=2)
        lock = resp.json()
    except Exception:
        pass

    try:
        resp = http_requests.get(f"{DBT_RUNNER_URL}/dbt/intents", timeout=2)
        intents = resp.json().get("intents", {})
    except Exception:
        pass

    try:
        resp = http_requests.get(f"{DBT_RUNNER_URL}/dbt/docs/status", timeout=2)
        docs_available = resp.json().get("available", False)
    except Exception:
        pass

    return {"lock": lock, "intents": intents, "docs_available": docs_available}


@router.get("/dbt", response_class=HTMLResponse)
def dbt_dashboard(request: Request):
    ctx = _fetch_dbt_context()
    return templates.TemplateResponse(request=request, name="admin/dbt.html", context={
        "request": request,
        **ctx,
        "docs_url": DBT_DOCS_URL,
        "trigger_result": None,
        "docs_result": None,
    })


@router.post("/dbt/trigger", response_class=HTMLResponse)
def dbt_trigger(
    request: Request,
    intent: str = Form(None),
    select_override: str = Form(""),
    full_refresh: bool = Form(False),
    fail_fast: bool = Form(False),
):
    payload: dict = {"full_refresh": full_refresh, "fail_fast": fail_fast}
    if select_override.strip():
        payload["select"] = [t.strip() for t in select_override.split() if t.strip()]
    elif intent:
        payload["intent"] = intent

    trigger_result = None
    trigger_ok = False
    try:
        resp = http_requests.post(f"{DBT_RUNNER_URL}/dbt/build", json=payload, timeout=300)
        trigger_result = resp.json()
        trigger_ok = resp.status_code == 200
    except Exception as e:
        trigger_result = {"error": str(e)}

    ctx = _fetch_dbt_context()
    return templates.TemplateResponse(request=request, name="admin/dbt.html", context={
        "request": request,
        **ctx,
        "docs_url": DBT_DOCS_URL,
        "trigger_result": trigger_result,
        "trigger_ok": trigger_ok,
        "docs_result": None,
    })


@router.post("/dbt/intents", response_class=HTMLResponse)
def dbt_intent_upsert(
    request: Request,
    intent_name: str = Form(...),
    select_args: str = Form(...),
):
    """Create or update an intent via the admin UI."""
    tokens = [t.strip() for t in select_args.split() if t.strip()]
    try:
        resp = http_requests.post(
            f"{DBT_RUNNER_URL}/dbt/intents",
            json={"intent_name": intent_name.strip(), "select_args": tokens},
            timeout=5,
        )
        resp.raise_for_status()
    except Exception:
        pass
    return RedirectResponse(url="/admin/dbt", status_code=303)


@router.post("/dbt/intents/{intent_name}/delete", response_class=HTMLResponse)
def dbt_intent_delete(request: Request, intent_name: str):
    try:
        http_requests.delete(f"{DBT_RUNNER_URL}/dbt/intents/{intent_name}", timeout=5)
    except Exception:
        pass
    return RedirectResponse(url="/admin/dbt", status_code=303)


@router.post("/dbt/docs/generate", response_class=HTMLResponse)
def dbt_docs_generate(request: Request):
    docs_result = None
    docs_ok = False
    try:
        resp = http_requests.post(f"{DBT_RUNNER_URL}/dbt/docs/generate", timeout=120)
        docs_result = resp.json()
        docs_ok = resp.status_code == 200 and docs_result.get("ok", False)
    except Exception as e:
        docs_result = {"error": str(e)}

    ctx = _fetch_dbt_context()
    return templates.TemplateResponse(request=request, name="admin/dbt.html", context={
        "request": request,
        **ctx,
        "docs_url": DBT_DOCS_URL,
        "trigger_result": None,
        "docs_result": docs_result,
        "docs_ok": docs_ok,
    })


# ---------------------------------------------------------------------------
# Log viewer
# ---------------------------------------------------------------------------

_OPS_LOG_PATH = os.getenv("LOG_PATH", "/usr/app/logs/app.log")


@router.get("/logs", response_class=HTMLResponse)
def view_logs(request: Request, lines: int = 200):
    scraper_lines: list[str] = []
    dbt_lines: list[str] = []
    ops_lines: list[str] = []

    try:
        resp = http_requests.get(f"{SCRAPER_URL}/logs?lines={lines}", timeout=5)
        scraper_lines = resp.json().get("lines", [])
    except Exception:
        pass

    try:
        resp = http_requests.get(f"{DBT_RUNNER_URL}/logs?lines={lines}", timeout=5)
        dbt_lines = resp.json().get("lines", [])
    except Exception:
        pass

    try:
        with open(_OPS_LOG_PATH) as f:
            ops_lines = f.readlines()[-lines:]
    except FileNotFoundError:
        pass

    return templates.TemplateResponse(request=request, name="admin/logs.html", context={
        "request": request,
        "scraper_lines": scraper_lines,
        "dbt_lines": dbt_lines,
        "ops_lines": ops_lines,
        "lines": lines,
    })


# ---------------------------------------------------------------------------
# Deploy panel
# ---------------------------------------------------------------------------

@router.get("/deploy", response_class=HTMLResponse)
def deploy_panel(request: Request):
    status = _intent_status()
    return templates.TemplateResponse(request=request, name="admin/deploy.html", context={
        "request": request,
        "status": status,
    })


@router.post("/deploy/start", response_class=HTMLResponse)
def deploy_start(request: Request):
    _set_intent("Admin UI")
    return RedirectResponse(url="/admin/deploy", status_code=303)


@router.post("/deploy/complete", response_class=HTMLResponse)
def deploy_complete(request: Request):
    _intent_release()
    return RedirectResponse(url="/admin/deploy", status_code=303)


# ---------------------------------------------------------------------------
# Edit config form
# ---------------------------------------------------------------------------

@router.get("/searches/{search_key}/edit", response_class=HTMLResponse)
def edit_search_form(request: Request, search_key: str):

    sql = """SELECT search_key, enabled, source, params, rotation_order, last_queued_at
            FROM search_configs WHERE search_key = %s;"""
    params = (search_key,)

    try:
        with db_cursor(error_context="Edit Searches", dict_cursor=True) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
    
    except Exception:
        return _db_error_response(request=request)
    
    if not row:
        return RedirectResponse(url="/admin/searches/", status_code=303)

    config = _row_to_dict(row)
    return templates.TemplateResponse(request=request, name="admin/form.html", context={
        "request": request,
        "editing": True,
        "config": config,
        "sort_options": SORT_OPTIONS,
        "error": None,
    })


# ---------------------------------------------------------------------------
# Create config (form POST)
# ---------------------------------------------------------------------------

@router.post("/searches/", response_class=HTMLResponse)
def create_search(
    request: Request,
    search_key: str = Form(...),
    makes: str = Form(...),
    models: str = Form(...),
    zip_code: str = Form(..., alias="zip"),
    radius_miles: int = Form(150),
    max_listings: int = Form(2000),
    max_safety_pages: int = Form(500),
    scope_local: bool = Form(False),
    scope_national: bool = Form(False),
    sort_rotation: list[str] = Form([]),
    rotation_order: Optional[int] = Form(None),
    enabled: bool = Form(False),
):
    key = _slug(search_key)
    scopes = []
    if scope_local:
        scopes.append("local")
    if scope_national:
        scopes.append("national")
    if not scopes:
        scopes = ["local", "national"]

    rotation = [s for s in sort_rotation if s in SORT_KEYS] or None
    sort_order = rotation[0] if rotation else "best_match_desc"

    try:
        params = SearchConfigParams(
            makes=_parse_comma_list(makes),
            models=_parse_comma_list(models),
            zip=zip_code,
            radius_miles=radius_miles,
            scopes=scopes,
            max_listings=max_listings,
            max_safety_pages=max_safety_pages,
            sort_order=sort_order,
            sort_rotation=rotation,
            rotation_slot=rotation_order,
        )
    except Exception as e:
        return templates.TemplateResponse(request=request, name="admin/form.html", context={
            "request": request,
            "editing": False,
            "config": {"search_key": key, "enabled": enabled, "params": {
                "makes": _parse_comma_list(makes), "models": _parse_comma_list(models),
                "zip": zip_code, "radius_miles": radius_miles, "scopes": scopes,
                "max_listings": max_listings, "max_safety_pages": max_safety_pages,
                "sort_rotation": sort_rotation,
            }},
            "sort_options": SORT_OPTIONS,
            "error": str(e),
        }, status_code=422)
    

    sql = """INSERT INTO search_configs (
                search_key,
                enabled,
                params,
                rotation_order,
                rotation_slot,
                created_at,
                updated_at
            )
             VALUES (%s, %s, %s::jsonb, %s, %s, now(), now());"""

    sql_params = (
        key, 
        enabled, 
        json.dumps(
        params.model_dump()), 
        rotation_order, 
        params.rotation_slot)

    try:
        with db_cursor(error_context="Create-Search") as cur:
            cur.execute(sql, sql_params)

    except Exception as e:
        if "duplicate key" in str(e).lower():
            return templates.TemplateResponse(request=request, name="admin/form.html", context={
                "request": request,
                "editing": False,
                "config": {"search_key": key, "enabled": enabled, "params": params.model_dump()},
                "sort_options": SORT_OPTIONS,
                "error": f"Search key '{key}' already exists.",
            }, status_code=422)
        return _db_error_response(request=request)

    return RedirectResponse(url="/admin/searches/", status_code=303)


# ---------------------------------------------------------------------------
# Update config (form POST)
# ---------------------------------------------------------------------------

@router.post("/searches/{search_key}", response_class=HTMLResponse)
def update_search(
    request: Request,
    search_key: str,
    makes: str = Form(...),
    models: str = Form(...),
    zip_code: str = Form(..., alias="zip"),
    radius_miles: int = Form(150),
    max_listings: int = Form(2000),
    max_safety_pages: int = Form(500),
    scope_local: bool = Form(False),
    scope_national: bool = Form(False),
    sort_rotation: list[str] = Form([]),
    rotation_order: Optional[int] = Form(None),
    enabled: bool = Form(False),
):
    scopes = []
    if scope_local:
        scopes.append("local")
    if scope_national:
        scopes.append("national")
    if not scopes:
        scopes = ["local", "national"]

    rotation = [s for s in sort_rotation if s in SORT_KEYS] or None
    sort_order = rotation[0] if rotation else "best_match_desc"

    try:
        params = SearchConfigParams(
            makes=_parse_comma_list(makes),
            models=_parse_comma_list(models),
            zip=zip_code,
            radius_miles=radius_miles,
            scopes=scopes,
            max_listings=max_listings,
            max_safety_pages=max_safety_pages,
            sort_order=sort_order,
            sort_rotation=rotation,
            rotation_slot=rotation_order,
        )
    except Exception as e:
        return templates.TemplateResponse(request=request, name="admin/form.html", context={
            "request": request,
            "editing": True,
            "config": {"search_key": search_key, "enabled": enabled, "params": {
                "makes": _parse_comma_list(makes), "models": _parse_comma_list(models),
                "zip": zip_code, "radius_miles": radius_miles, "scopes": scopes,
                "max_listings": max_listings, "max_safety_pages": max_safety_pages,
                "sort_rotation": sort_rotation,
            }},
            "sort_options": SORT_OPTIONS,
            "error": str(e),
        }, status_code=422)
    

    sql = """UPDATE search_configs
             SET
                enabled = %s,
                params = %s::jsonb,
                rotation_order = %s,
                rotation_slot = %s,
                updated_at = now()
             WHERE search_key = %s;"""
    sql_params = (enabled, 
                  json.dumps(params.model_dump()), 
                  rotation_order, 
                  params.rotation_slot, 
                  search_key)

    try:
        with db_cursor(error_context="Update-Search") as cur:
            cur.execute(sql, sql_params)
    except Exception:
        return _db_error_response(request=request)

    return RedirectResponse(url="/admin/searches/", status_code=303)


# ---------------------------------------------------------------------------
# Toggle enable/disable
# ---------------------------------------------------------------------------

@router.post("/searches/{search_key}/toggle")
def toggle_search(request: Request, search_key: str):

    sql = """UPDATE search_configs SET enabled = NOT enabled, updated_at = now()
             WHERE search_key = %s;"""
    params = (search_key,)

    try:
        with db_cursor(error_context="Toggle-Search") as cur:
            cur.execute(sql, params)
    except Exception:
        return _db_error_response(request=request)
    
    return RedirectResponse(url="/admin/searches/", status_code=303)


# ---------------------------------------------------------------------------
# Delete (soft — disable + rename to prevent key reuse conflicts)
# ---------------------------------------------------------------------------

@router.post("/searches/{search_key}/delete")
def delete_search(request: Request, search_key: str):
    deleted_key = f"_deleted_{search_key}_{int(datetime.now(UTC).timestamp())}"

    sql = """UPDATE search_configs SET enabled = false, search_key = %s, updated_at = now()
            WHERE search_key = %s;"""
    params = (deleted_key, search_key)

    try:
        with db_cursor(error_context="Delete-Search") as cur:
            cur.execute(sql, params)
    except Exception:
        return _db_error_response(request=request)

    return RedirectResponse(url="/admin/searches/", status_code=303)
