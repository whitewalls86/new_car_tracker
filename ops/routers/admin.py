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

from ops.queries import (
    INSERT_SEARCH_CONFIG,
    RETIRE_SEARCH_CONFIG,
    SELECT_SEARCH_CONFIG_BY_KEY,
    SELECT_SEARCH_CONFIGS,
    TOGGLE_SEARCH_CONFIG_ENABLED,
    UPDATE_SEARCH_CONFIG,
)
from shared.db import db_cursor

from ..models.search_config import SORT_KEYS, SORT_OPTIONS, SearchConfigParams
from ..routers.deploy import _intent_release, _intent_status, _set_intent

DBT_RUNNER_URL = os.environ.get("DBT_RUNNER_URL", "http://dbt_runner:8080")
DBT_DOCS_URL = os.environ.get("DBT_DOCS_URL", "http://localhost:8081/dbt-docs/")

# Plan 162 Stage AA: `SCRAPER_URL` lived here for one caller, the `GET
# /logs` fetch that route stopped serving in May. Plan 104 put every
# container's logs in Loki, so the panel links there instead of showing an
# empty pane.
GRAFANA_LOGS_URL = os.environ.get(
    "GRAFANA_LOGS_URL", "https://cartracker.info/grafana/explore"
)


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


def _not_found_response(request: Request, message: str):
    """The answer when a mutation matched no row.

    Its absence is what Plan 162 Stage Y found: these routes redirected to the
    list page whether the key existed or not, so a typo and a real edit were
    indistinguishable to the operator and to the suite.
    """
    return templates.TemplateResponse(request=request, name="admin/error.html", context={
        "request": request,
        "heading": "Not Found",
        "message": message,
    }, status_code=404)

# ---------------------------------------------------------------------------
# Search config list
# ---------------------------------------------------------------------------

@router.get(
    "/searches/",
    response_class=HTMLResponse,
    responses={
        503: {"description": "Database unavailable."},
    },
)
def list_searches(request: Request):
    sql = SELECT_SEARCH_CONFIGS
    
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
# dbt action panel
# ---------------------------------------------------------------------------

def _fetch_dbt_context() -> dict:
    """What the dbt panel can actually learn from dbt_runner.

    Plan 162 Stage AA. This asked for three things and two of them had not
    existed since April: ``GET /dbt/lock`` and ``GET /dbt/intents`` were removed
    by `9f08336` while the dbt layer was rebuilt, and every call here was
    wrapped in `except Exception: pass`, so the panel rendered an empty lock and
    an empty intent list and looked like it was working. `caller_endpoints`
    resolves this module to three endpoints; those two matched no contract path
    at all.

    **The cadence list comes from dbt_runner, not from this service's disk.**
    An earlier version of this read ``dbt/selectors.yml`` directly -- which
    works, because `ops` ships the whole tree -- and was still wrong:
    ``/dbt/build`` validates a selector against *dbt_runner's* copy, and `ops`
    deploys last and alone, so for the length of a deploy the panel could offer
    a name the build would then refuse.

    **``/ready`` is the lock, and it always was.** It answers
    ``{ready, active_jobs, oldest_started_at}`` and 503 while a build is
    running -- the same evidence the deploy drain reads through
    ``dbt_runner_jobs``. A 503 here means *busy*, not *error*: it is a code that
    endpoint declares, and treating a declared refusal as a failure is the
    defect this stage is named for, one level up.
    """
    busy = None
    docs_available = False
    selectors: list[str] = []

    try:
        resp = http_requests.get(f"{DBT_RUNNER_URL}/ready", timeout=2)
        if resp.status_code == 503:
            busy = (resp.json().get("detail") or {})
        else:
            resp.raise_for_status()
            busy = resp.json()
    except Exception:
        busy = None

    try:
        resp = http_requests.get(f"{DBT_RUNNER_URL}/dbt/selectors", timeout=2)
        resp.raise_for_status()
        selectors = resp.json().get("selectors", [])
    except Exception:
        pass

    try:
        resp = http_requests.get(f"{DBT_RUNNER_URL}/dbt/docs/status", timeout=2)
        resp.raise_for_status()
        docs_available = resp.json().get("available", False)
    except Exception:
        pass

    return {
        "build_state": busy,
        "selectors": selectors,
        "docs_available": docs_available,
    }


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
    selector: str = Form(None),
    select_override: str = Form(""),
    full_refresh: bool = Form(False),
    fail_fast: bool = Form(False),
):
    payload: dict = {"full_refresh": full_refresh, "fail_fast": fail_fast}
    if select_override.strip():
        payload["select"] = [t.strip() for t in select_override.split() if t.strip()]
    elif selector:
        # `intent` until Plan 162 Stage AA, and dead at both ends: this set it
        # and `dbt_build` read only select/exclude/full_refresh/fail_fast.
        payload["selector"] = selector

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
    """This service's own log. The other two are read in Grafana.

    Plan 162 Stage AA. This fetched `GET /logs` from `scraper` and from
    `dbt_runner`, and neither route has existed since `d88a41e` standardised
    logging in May; both calls were wrapped in `except Exception: pass`, so the
    page rendered two empty panes and looked like two quiet services. Plan 104
    put every container's logs in Loki, which is where a reader should be
    sent -- a log viewer that silently shows nothing is worse than one that
    says where to look.
    """
    ops_lines: list[str] = []
    try:
        with open(_OPS_LOG_PATH, encoding="utf-8") as f:
            ops_lines = f.readlines()[-lines:]
    except FileNotFoundError:
        pass

    return templates.TemplateResponse(request=request, name="admin/logs.html", context={
        "request": request,
        "ops_lines": ops_lines,
        "lines": lines,
        "grafana_url": GRAFANA_LOGS_URL,
    })


# ---------------------------------------------------------------------------
# Deploy panel
# ---------------------------------------------------------------------------

def _deploy_refusal(request: Request, result, headline: str) -> HTMLResponse:
    """Render a non-ok intent outcome on the deploy panel, with its own status.

    The codes are literals in three branches rather than a lookup, and that is
    not style. `test_every_route_declares_the_statuses_it_can_return` resolves
    one level into a same-module helper and reads the codes it can *see*; a
    `dict.get(...)` is opaque to it, and it was opaque to a reader too. The
    same three codes the API pair answers for the same `IntentResult` status,
    so the two surfaces cannot disagree about what `locked` means.
    """
    context = {
        "request": request,
        "status": _intent_status(),
        "refusal": {
            "headline": headline,
            "outcome": result.status,
            "detail": result.detail,
        },
    }
    if result.status in {"locked", "invalid"}:
        return templates.TemplateResponse(
            request=request, name="admin/deploy.html", status_code=409, context=context,
        )
    if result.status == "unavailable":
        return templates.TemplateResponse(
            request=request, name="admin/deploy.html", status_code=503, context=context,
        )
    return templates.TemplateResponse(
        request=request, name="admin/deploy.html", status_code=500, context=context,
    )


@router.get("/deploy", response_class=HTMLResponse)
def deploy_panel(request: Request):
    status = _intent_status()
    return templates.TemplateResponse(request=request, name="admin/deploy.html", context={
        "request": request,
        "status": status,
        "refusal": None,
    })


# The admin buttons are `/deploy/request` and `/deploy/release` rather than
# `/deploy/start` and `/deploy/complete`, and the rename is the repair rather
# than cosmetics. `ops` mounts `ops/routers/deploy.py` bare and this router
# under `/admin`, so the two pairs carried **identical decorator strings** --
# `@router.post("/deploy/start")` in both files -- and no request could be
# attributed to one handler or the other. Four waivers stood on that: these two
# and, because the collision taints both sides, the two API handlers that
# `scripts/redeploy.sh` actually drives.
#
# The names are also the truer ones. The API *starts a deploy*; the button asks
# the coordination record for the intent, which is a request that can be
# refused.

@router.post(
    "/deploy/request",
    response_class=HTMLResponse,
    # The success path redirects, so 303 is the default rather than the 200
    # FastAPI would otherwise declare. These two routes were invisible to
    # `test_every_status_code_a_route_can_produce_is_asserted` while they
    # shared a decorator path with the API pair and were waived as
    # ambiguous; disambiguating them surfaced a 200 neither can produce.
    status_code=303,
    responses={
        303: {"description": "Intent recorded; redirect to the deploy panel."},
        409: {"description": "Another coordination holds the record."},
        500: {"description": "Postgres refused the write; the detail names why."},
        503: {"description": "Database unavailable."},
    },
)
def deploy_start(request: Request):
    """Ask for deploy intent, and say what happened.

    Plan 162 Stage AA, G27. This called `_set_intent("Admin UI")` and discarded
    the answer, then redirected 303 unconditionally -- so an operator who
    clicked the button saw the same page whether the intent was recorded,
    another coordination held the row, or Postgres refused the write. Stage K
    widened `IntentResult` to carry a `detail` for exactly this distinction and
    the panel never read it.

    The five outcomes map as the API pair maps them, because they are the same
    five from the same helper and an operator should not have to learn a second
    vocabulary for them.
    """
    result = _set_intent("Admin UI")
    if result.status == "ok":
        return RedirectResponse(url="/admin/deploy", status_code=303)
    return _deploy_refusal(request, result, "Deploy intent could not be recorded")


@router.post(
    "/deploy/release",
    response_class=HTMLResponse,
    # The success path redirects, so 303 is the default rather than the 200
    # FastAPI would otherwise declare. These two routes were invisible to
    # `test_every_status_code_a_route_can_produce_is_asserted` while they
    # shared a decorator path with the API pair and were waived as
    # ambiguous; disambiguating them surfaced a 200 neither can produce.
    status_code=303,
    responses={
        303: {"description": "Intent released; redirect to the deploy panel."},
        409: {"description": "Another coordination holds the record."},
        500: {"description": "Postgres refused the write; the detail names why."},
        503: {"description": "Database unavailable."},
    },
)
def deploy_complete(request: Request):
    """Release the intent, and say what happened.

    The quieter of the two failures, and the one `_intent_release`'s own
    docstring records: a release that fails leaves every gated DAG parked, and
    until now the button reported that exactly as it reported success.
    """
    result = _intent_release()
    if result.status == "ok":
        return RedirectResponse(url="/admin/deploy", status_code=303)
    return _deploy_refusal(request, result, "Deploy intent could not be released")


# ---------------------------------------------------------------------------
# Edit config form
# ---------------------------------------------------------------------------

@router.get(
    "/searches/{search_key}/edit",
    response_class=HTMLResponse,
    responses={
        303: {"description": "No such search config; redirect to the list."},
        503: {"description": "Database unavailable."},
    },
)
def edit_search_form(request: Request, search_key: str):

    sql = SELECT_SEARCH_CONFIG_BY_KEY
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

@router.post(
    "/searches/",
    response_class=HTMLResponse,
    # Plan 162 Stage AA: the success path redirects, so 303 is the
    # declared default rather than the 200 FastAPI would otherwise put in
    # the contract -- a code no exit of this handler produces.
    status_code=303,
    responses={
        303: {"description": "Created; redirect to the search list."},
        422: {"description": "The submitted form is not a valid search config."},
        503: {"description": "Database unavailable."},
    },
)
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
    

    sql = INSERT_SEARCH_CONFIG

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

@router.post(
    "/searches/{search_key}",
    response_class=HTMLResponse,
    # Plan 162 Stage AA: the success path redirects, so 303 is the
    # declared default rather than the 200 FastAPI would otherwise put in
    # the contract -- a code no exit of this handler produces.
    status_code=303,
    responses={
        303: {"description": "Change applied; redirect to the search list."},
        404: {"description": "No search config with that key; nothing was changed."},
        422: {"description": "The submitted form is not a valid search config."},
        503: {"description": "Database unavailable."},
    },
)
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
    

    sql = UPDATE_SEARCH_CONFIG
    sql_params = (enabled, 
                  json.dumps(params.model_dump()), 
                  rotation_order, 
                  params.rotation_slot, 
                  search_key)

    try:
        with db_cursor(error_context="Update-Search") as cur:
            cur.execute(sql, sql_params)
            matched = cur.rowcount
    except Exception:
        return _db_error_response(request=request)

    if not matched:
        return _not_found_response(request, f"No search config named {search_key!r}.")

    return RedirectResponse(url="/admin/searches/", status_code=303)


# ---------------------------------------------------------------------------
# Toggle enable/disable
# ---------------------------------------------------------------------------

@router.post(
    "/searches/{search_key}/toggle",
    response_class=HTMLResponse,
    # Plan 162 Stage AA: the success path redirects, so 303 is the
    # declared default rather than the 200 FastAPI would otherwise put in
    # the contract -- a code no exit of this handler produces.
    status_code=303,
    responses={
        303: {"description": "Change applied; redirect to the search list."},
        404: {"description": "No search config with that key; nothing was changed."},
        503: {"description": "Database unavailable."},
    },
)
def toggle_search(request: Request, search_key: str):

    sql = TOGGLE_SEARCH_CONFIG_ENABLED
    params = (search_key,)

    try:
        with db_cursor(error_context="Toggle-Search") as cur:
            cur.execute(sql, params)
            matched = cur.rowcount
    except Exception:
        return _db_error_response(request=request)

    if not matched:
        return _not_found_response(request, f"No search config named {search_key!r}.")
    
    return RedirectResponse(url="/admin/searches/", status_code=303)


# ---------------------------------------------------------------------------
# Delete (soft — disable + rename to prevent key reuse conflicts)
# ---------------------------------------------------------------------------

@router.post(
    "/searches/{search_key}/delete",
    response_class=HTMLResponse,
    # Plan 162 Stage AA: the success path redirects, so 303 is the
    # declared default rather than the 200 FastAPI would otherwise put in
    # the contract -- a code no exit of this handler produces.
    status_code=303,
    responses={
        303: {"description": "Change applied; redirect to the search list."},
        404: {"description": "No search config with that key; nothing was changed."},
        503: {"description": "Database unavailable."},
    },
)
def delete_search(request: Request, search_key: str):
    deleted_key = f"_deleted_{search_key}_{int(datetime.now(UTC).timestamp())}"

    sql = RETIRE_SEARCH_CONFIG
    params = (deleted_key, search_key)

    try:
        with db_cursor(error_context="Delete-Search") as cur:
            cur.execute(sql, params)
            matched = cur.rowcount
    except Exception:
        return _db_error_response(request=request)

    if not matched:
        return _not_found_response(request, f"No search config named {search_key!r}.")

    return RedirectResponse(url="/admin/searches/", status_code=303)
