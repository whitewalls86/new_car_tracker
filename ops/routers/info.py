"""
The public landing page. No authentication required; Caddy routes both paths
below to ops without forward_auth.

Plan 138 Stage 2 moved the page from ``/info`` to ``/``. ``/info`` is kept as a
permanent redirect rather than retired, because it is the URL printed on a
resume, a LinkedIn profile and a GitHub profile -- copies this repository cannot
edit. It has to keep resolving for as long as those do.
"""
import os

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ops.public_stats import public_stats_cache
from ops.static_assets import asset_url

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

# Stage 3c. Every asset the page loads goes through this, so the one-year
# immutable cache on /static_ops/* cannot outlive a deploy that changed one.
templates.env.globals["asset_url"] = asset_url


@router.get("/info")
def info_redirect() -> RedirectResponse:
    """The pre-Stage-2 landing URL, forwarded to its canonical replacement.

    308 rather than 301: it preserves the method, and unlike 302 it tells a
    crawler to move the indexed URL rather than to keep asking. The landing page
    also carries a canonical link to ``/``, so the two agree.
    """
    return RedirectResponse(url="/", status_code=308)


@router.get("/", response_class=HTMLResponse)
def info_page(request: Request):
    snapshot = public_stats_cache.get()

    return templates.TemplateResponse(
        request=request,
        name="info.html",
        context={"request": request, "stats": snapshot.stats, "stats_snapshot": snapshot},
    )
