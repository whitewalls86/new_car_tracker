"""
Public /info route — renders the CarTracker portfolio landing page.
No authentication required; Caddy routes /info without forward_auth.
"""
import logging
import os
from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from shared.db import db_cursor

logger = logging.getLogger(__name__)

router = APIRouter()

_BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
templates = Jinja2Templates(directory=os.path.join(_BASE_DIR, "templates"))


def _load_stats() -> dict:
    """
    Pull live counts from the DB for the landing page.
    Returns a dict with keys: vins_tracked, price_observations,
    artifacts_today, last_pipeline_run.
    Any value that fails to load is set to None and silently omitted.
    """
    stats: dict = {}

    try:
        with db_cursor(error_context="info: vins_tracked") as cur:
            cur.execute("SELECT COUNT(DISTINCT vin) FROM ops.vin_to_listing WHERE vin IS NOT NULL")
            row = cur.fetchone()
            if row:
                stats["vins_tracked"] = row[0]
    except Exception:
        logger.debug("info stats: vins_tracked query failed", exc_info=True)

    try:
        with db_cursor(error_context="info: price_observations") as cur:
            cur.execute("SELECT COUNT(*) FROM ops.price_observations")
            row = cur.fetchone()
            if row:
                stats["price_observations"] = row[0]
    except Exception:
        logger.debug("info stats: price_observations query failed", exc_info=True)

    try:
        with db_cursor(error_context="info: artifacts_today") as cur:
            cur.execute(
                "SELECT COUNT(*) FROM ops.artifacts_queue WHERE fetched_at >= CURRENT_DATE"
            )
            row = cur.fetchone()
            if row:
                stats["artifacts_today"] = row[0]
    except Exception:
        logger.debug("info stats: artifacts_today query failed", exc_info=True)

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
