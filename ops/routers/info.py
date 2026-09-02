"""
Public /info route — renders the CarTracker portfolio landing page.
No authentication required; Caddy routes /info without forward_auth.
"""
import os

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from ops.public_stats import public_stats_cache

router = APIRouter()

_BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
templates = Jinja2Templates(directory=os.path.join(_BASE_DIR, "templates"))

def _fmt_stat(value: int | float) -> str:
    """Format a stat number for display: abbreviate millions/thousands."""
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if value >= 10_000:
        return f"{value / 1_000:.0f}K"
    if value >= 1_000:
        return f"{value / 1_000:.1f}K"
    return f"{value:,}"


templates.env.filters["fmt_stat"] = _fmt_stat


@router.get("/info", response_class=HTMLResponse)
def info_page(request: Request):
    snapshot = public_stats_cache.get()

    return templates.TemplateResponse(
        request=request,
        name="info.html",
        context={"request": request, "stats": snapshot.stats, "stats_snapshot": snapshot},
    )
