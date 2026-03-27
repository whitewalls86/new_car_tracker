"""
Admin UI routes for managing search_configs, viewing run history, and operating dbt.
"""
import json
import os
import re
from datetime import datetime, UTC
from typing import Optional
import requests as http_requests
from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from db import get_pool
from models.search_config import SearchConfigParams, SORT_OPTIONS, SORT_KEYS

DBT_RUNNER_URL = os.environ.get("DBT_RUNNER_URL", "http://dbt_runner:8080")
DBT_DOCS_URL = os.environ.get("DBT_DOCS_URL", "http://localhost:8081/dbt-docs/")

router = APIRouter()
templates = Jinja2Templates(directory="templates")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _slug(text: str) -> str:
    """Convert text to a URL-safe slug for search_key."""
    return re.sub(r"[^a-z0-9_-]", "", text.lower().replace(" ", "-").replace("/", "-"))


def _parse_comma_list(raw: str) -> list[str]:
    """Split comma-separated string into a list of trimmed, non-empty strings."""
    return [s.strip() for s in raw.split(",") if s.strip()]


def _row_to_dict(row) -> dict:
    """Convert an asyncpg Record to a dict with params unpacked."""
    d = dict(row)
    if isinstance(d.get("params"), str):
        d["params"] = json.loads(d["params"])
    return d


def _stringify_uuids(d: dict) -> dict:
    """Convert any asyncpg UUID values to plain strings so templates can slice/compare them."""
    return {k: str(v) if hasattr(v, 'hex') and hasattr(v, 'bytes') else v for k, v in d.items()}


# ---------------------------------------------------------------------------
# List view
# ---------------------------------------------------------------------------

@router.get("/searches/", response_class=HTMLResponse)
async def list_searches(request: Request):
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT search_key, enabled, source, params, rotation_order, last_queued_at, created_at, updated_at "
        "FROM search_configs ORDER BY enabled DESC, rotation_order NULLS LAST, search_key"
    )
    configs = [_row_to_dict(r) for r in rows]
    return templates.TemplateResponse(request=request, name="admin/list.html", context={
        "request": request,
        "configs": configs,
    })


# ---------------------------------------------------------------------------
# New config form
# ---------------------------------------------------------------------------

@router.get("/searches/new", response_class=HTMLResponse)
async def new_search_form(request: Request):
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
async def list_runs(request: Request):
    pool = await get_pool()
    rows = await pool.fetch("""
        SELECT run_id, started_at, finished_at, status, trigger,
               progress_count, total_count, error_count, last_error, notes
        FROM runs
        ORDER BY started_at DESC
        LIMIT 20
    """)
    runs = [_stringify_uuids(dict(r)) for r in rows]
    return templates.TemplateResponse(request=request, name="admin/runs.html", context={
        "request": request,
        "runs": runs,
    })


@router.get("/runs/{run_id}", response_class=HTMLResponse)
async def run_detail(request: Request, run_id: str):
    pool = await get_pool()
    run = await pool.fetchrow("""
        SELECT run_id, started_at, finished_at, status, trigger,
               progress_count, total_count, error_count, last_error, notes
        FROM runs WHERE run_id = $1
    """, run_id)
    if not run:
        return RedirectResponse(url="/admin/runs", status_code=303)
    jobs = await pool.fetch("""
        SELECT job_id, search_key, scope, status, created_at,
               started_at, completed_at, artifact_count, error, retry_count
        FROM scrape_jobs
        WHERE run_id = $1
        ORDER BY created_at
    """, run_id)
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
async def dbt_dashboard(request: Request):
    ctx = _fetch_dbt_context()
    return templates.TemplateResponse(request=request, name="admin/dbt.html", context={
        "request": request,
        **ctx,
        "docs_url": DBT_DOCS_URL,
        "trigger_result": None,
        "docs_result": None,
    })


@router.post("/dbt/trigger", response_class=HTMLResponse)
async def dbt_trigger(
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
async def dbt_intent_upsert(
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
    except Exception as e:
        pass  # errors surface on page reload via intents fetch
    return RedirectResponse(url="/admin/dbt", status_code=303)


@router.post("/dbt/intents/{intent_name}/delete", response_class=HTMLResponse)
async def dbt_intent_delete(request: Request, intent_name: str):
    try:
        http_requests.delete(f"{DBT_RUNNER_URL}/dbt/intents/{intent_name}", timeout=5)
    except Exception:
        pass
    return RedirectResponse(url="/admin/dbt", status_code=303)


@router.post("/dbt/docs/generate", response_class=HTMLResponse)
async def dbt_docs_generate(request: Request):
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
# Edit config form
# ---------------------------------------------------------------------------

@router.get("/searches/{search_key}/edit", response_class=HTMLResponse)
async def edit_search_form(request: Request, search_key: str):
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT search_key, enabled, source, params, rotation_order, last_queued_at FROM search_configs WHERE search_key = $1",
        search_key,
    )
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
async def create_search(
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

    pool = await get_pool()
    try:
        await pool.execute(
            "INSERT INTO search_configs (search_key, enabled, params, rotation_order, created_at, updated_at) "
            "VALUES ($1, $2, $3::jsonb, $4, now(), now())",
            key, enabled, json.dumps(params.model_dump()), rotation_order,
        )
    except Exception as e:
        if "duplicate key" in str(e).lower():
            error = f"Search key '{key}' already exists."
        else:
            error = str(e)
        return templates.TemplateResponse(request=request, name="admin/form.html", context={
            "request": request,
            "editing": False,
            "config": {"search_key": key, "enabled": enabled, "params": params.model_dump()},
            "sort_options": SORT_OPTIONS,
            "error": error,
        }, status_code=422)

    return RedirectResponse(url="/admin/searches/", status_code=303)


# ---------------------------------------------------------------------------
# Update config (form POST)
# ---------------------------------------------------------------------------

@router.post("/searches/{search_key}", response_class=HTMLResponse)
async def update_search(
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
            rotation_slot=rotation_order
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

    pool = await get_pool()
    await pool.execute(
        "UPDATE search_configs SET enabled = $1, params = $2::jsonb, rotation_order = $3, updated_at = now() "
        "WHERE search_key = $4",
        enabled, json.dumps(params.model_dump()), rotation_order, search_key,
    )
    return RedirectResponse(url="/admin/searches/", status_code=303)


# ---------------------------------------------------------------------------
# Toggle enable/disable
# ---------------------------------------------------------------------------

@router.post("/searches/{search_key}/toggle")
async def toggle_search(search_key: str):
    pool = await get_pool()
    await pool.execute(
        "UPDATE search_configs SET enabled = NOT enabled, updated_at = now() "
        "WHERE search_key = $1",
        search_key,
    )
    return RedirectResponse(url="/admin/searches/", status_code=303)


# ---------------------------------------------------------------------------
# Delete (soft — disable + rename to prevent key reuse conflicts)
# ---------------------------------------------------------------------------

@router.post("/searches/{search_key}/delete")
async def delete_search(search_key: str):
    pool = await get_pool()
    deleted_key = f"_deleted_{search_key}_{int(datetime.now(UTC).timestamp())}"
    await pool.execute(
        "UPDATE search_configs SET enabled = false, search_key = $1, updated_at = now() "
        "WHERE search_key = $2",
        deleted_key, search_key,
    )
    return RedirectResponse(url="/admin/searches/", status_code=303)
