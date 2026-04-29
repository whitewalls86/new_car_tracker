"""
Public /info route — renders the CarTracker portfolio landing page.
No authentication required; Caddy routes /info without forward_auth.
"""
import logging
import os
from datetime import datetime, timezone

import duckdb
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from shared.db import db_cursor

logger = logging.getLogger(__name__)

router = APIRouter()

_BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
templates = Jinja2Templates(directory=os.path.join(_BASE_DIR, "templates"))

_DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/data/analytics/analytics.duckdb")


def _load_stats() -> dict:
    """
    Pull live counts for the landing page.
    Active listings, price observations, and make/model pairs come from the
    DuckDB mart layer (mart_vehicle_snapshot). Last pipeline run comes from
    Postgres ops.artifacts_queue.
    Any value that fails to load is silently omitted.
    """
    stats: dict = {}

    try:
        with duckdb.connect(_DUCKDB_PATH, read_only=True) as con:
            row = con.execute(
                "SELECT COUNT(*) FROM main.mart_vehicle_snapshot WHERE listing_state = 'active'"
            ).fetchone()
            if row:
                stats["active_listings"] = row[0]
    except Exception:
        logger.debug("info stats: active_listings query failed", exc_info=True)

    try:
        with duckdb.connect(_DUCKDB_PATH, read_only=True) as con:
            row = con.execute(
                "SELECT COALESCE(SUM(total_price_observations), 0) FROM main.mart_vehicle_snapshot"
            ).fetchone()
            if row:
                stats["price_observations"] = row[0]
    except Exception:
        logger.debug("info stats: price_observations query failed", exc_info=True)

    try:
        with duckdb.connect(_DUCKDB_PATH, read_only=True) as con:
            row = con.execute(
                """
                SELECT COUNT(DISTINCT make || '|' || model)
                FROM main.mart_vehicle_snapshot
                WHERE make IS NOT NULL AND model IS NOT NULL
                """
            ).fetchone()
            if row:
                stats["make_model_pairs"] = row[0]
    except Exception:
        logger.debug("info stats: make_model_pairs query failed", exc_info=True)

    try:
        with db_cursor(error_context="info: last_pipeline_run") as cur:
            cur.execute(
                "SELECT MAX(fetched_at) FROM ops.artifacts_queue WHERE status = 'complete'"
            )
            row = cur.fetchone()
            if row and row[0]:
                ts: datetime = row[0]
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                stats["last_pipeline_run"] = ts.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        logger.debug("info stats: last_pipeline_run query failed", exc_info=True)

    return stats


@router.get("/info", response_class=HTMLResponse)
def info_page(request: Request):
    try:
        stats = _load_stats()
    except Exception:
        stats = {}

    return templates.TemplateResponse(
        request=request,
        name="info.html",
        context={"request": request, "stats": stats},
    )
