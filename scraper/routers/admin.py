"""
Admin UI routes for managing search_configs.
"""
import json
import re
from datetime import datetime, UTC
from typing import Optional
from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from db import get_pool
from models.search_config import SearchConfigParams, SORT_OPTIONS, SORT_KEYS

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


# ---------------------------------------------------------------------------
# List view
# ---------------------------------------------------------------------------

@router.get("/", response_class=HTMLResponse)
async def list_searches(request: Request):
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT search_key, enabled, source, params, rotation_order, last_queued_at, created_at, updated_at "
        "FROM search_configs ORDER BY enabled DESC, rotation_order NULLS LAST, search_key"
    )
    configs = [_row_to_dict(r) for r in rows]
    return templates.TemplateResponse("admin/list.html", {
        "request": request,
        "configs": configs,
    })


# ---------------------------------------------------------------------------
# New config form
# ---------------------------------------------------------------------------

@router.get("/new", response_class=HTMLResponse)
async def new_search_form(request: Request):
    return templates.TemplateResponse("admin/form.html", {
        "request": request,
        "editing": False,
        "config": None,
        "sort_options": SORT_OPTIONS,
        "error": None,
    })


# ---------------------------------------------------------------------------
# Edit config form
# ---------------------------------------------------------------------------

@router.get("/{search_key}/edit", response_class=HTMLResponse)
async def edit_search_form(request: Request, search_key: str):
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT search_key, enabled, source, params, rotation_order, last_queued_at FROM search_configs WHERE search_key = $1",
        search_key,
    )
    if not row:
        return RedirectResponse(url="/admin/", status_code=303)

    config = _row_to_dict(row)
    return templates.TemplateResponse("admin/form.html", {
        "request": request,
        "editing": True,
        "config": config,
        "sort_options": SORT_OPTIONS,
        "error": None,
    })


# ---------------------------------------------------------------------------
# Create config (form POST)
# ---------------------------------------------------------------------------

@router.post("/", response_class=HTMLResponse)
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
        )
    except Exception as e:
        return templates.TemplateResponse("admin/form.html", {
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
        return templates.TemplateResponse("admin/form.html", {
            "request": request,
            "editing": False,
            "config": {"search_key": key, "enabled": enabled, "params": params.model_dump()},
            "sort_options": SORT_OPTIONS,
            "error": error,
        }, status_code=422)

    return RedirectResponse(url="/admin/", status_code=303)


# ---------------------------------------------------------------------------
# Update config (form POST)
# ---------------------------------------------------------------------------

@router.post("/{search_key}", response_class=HTMLResponse)
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
        )
    except Exception as e:
        return templates.TemplateResponse("admin/form.html", {
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
    return RedirectResponse(url="/admin/", status_code=303)


# ---------------------------------------------------------------------------
# Toggle enable/disable
# ---------------------------------------------------------------------------

@router.post("/{search_key}/toggle")
async def toggle_search(search_key: str):
    pool = await get_pool()
    await pool.execute(
        "UPDATE search_configs SET enabled = NOT enabled, updated_at = now() "
        "WHERE search_key = $1",
        search_key,
    )
    return RedirectResponse(url="/admin/", status_code=303)


# ---------------------------------------------------------------------------
# Delete (soft — disable + rename to prevent key reuse conflicts)
# ---------------------------------------------------------------------------

@router.post("/{search_key}/delete")
async def delete_search(search_key: str):
    pool = await get_pool()
    deleted_key = f"_deleted_{search_key}_{int(datetime.now(UTC).timestamp())}"
    await pool.execute(
        "UPDATE search_configs SET enabled = false, search_key = $1, updated_at = now() "
        "WHERE search_key = $2",
        deleted_key, search_key,
    )
    return RedirectResponse(url="/admin/", status_code=303)
