"""Plan 138 Stage 2: the public routes that are not the landing page itself.

The weekly recaps, ``robots.txt`` and ``sitemap.xml``. All three are served
without authentication -- Caddy routes them straight to ops with no
``forward_auth`` -- and all three read from the same place: the directory
``scripts/build_public_recaps.py`` writes, mounted read-only from the checkout
by Stage 7.

**The sitemap is derived, not written.** Stage 2's exit says it must list
exactly the pages the Stage 1e generator rendered, once each. A hand-kept list
would drift the first week a recap was added, so the URL set is read off the
generated directory at request time: the generator is the only thing that can
change it.

**The recap pages already have a second URL.** ``handle /static_ops/*`` is
public, so ``/static_ops/generated/recaps/2026-08-30.html`` has answered 200
since the Stage 1e deploy. This module gives that content its canonical route;
the generator marks the canonical URL on each page so the static path resolves
to one address rather than competing with it.
"""
import os
import re

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse, Response

router = APIRouter()

PUBLIC_BASE_URL = "https://cartracker.info"

_BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RECAPS_DIR = os.path.join(_BASE_DIR, "static_ops", "generated", "recaps")

# A recap page is named for the Sunday its week ends on. The route only ever
# resolves that shape, which is also what keeps a slug from walking the path.
_SLUG_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Paths a crawler should not spend requests on. Every one of them answers with a
# Google sign-in page rather than content, so indexing them would publish the
# login screen as if it were this project.
_DISALLOWED = (
    "/dashboard",
    "/admin",
    "/request-access",
    "/oauth2",
    "/airflow",
    "/grafana",
    "/pgadmin",
    "/minio",
    "/health",
    "/metrics",
)


def published_slugs() -> list[str]:
    """The recap slugs that have a rendered page, newest first."""
    try:
        names = os.listdir(RECAPS_DIR)
    except OSError:
        return []
    slugs = [
        name[: -len(".html")]
        for name in names
        if name.endswith(".html") and _SLUG_RE.match(name[: -len(".html")])
    ]
    return sorted(slugs, reverse=True)


@router.get("/recaps", response_class=FileResponse)
def recap_index() -> FileResponse:
    index = os.path.join(RECAPS_DIR, "index.html")
    if not os.path.isfile(index):
        raise HTTPException(status_code=404, detail="No recaps have been published.")
    return FileResponse(index, media_type="text/html")


@router.get("/recaps/{slug}", response_class=FileResponse)
def recap_page(slug: str) -> FileResponse:
    if not _SLUG_RE.match(slug):
        raise HTTPException(status_code=404, detail="No such recap.")
    page = os.path.join(RECAPS_DIR, f"{slug}.html")
    if not os.path.isfile(page):
        raise HTTPException(status_code=404, detail="No such recap.")
    return FileResponse(page, media_type="text/html")


@router.get("/robots.txt", response_class=PlainTextResponse)
def robots() -> PlainTextResponse:
    lines = ["User-agent: *", "Allow: /"]
    lines += [f"Disallow: {path}" for path in _DISALLOWED]
    lines += ["", f"Sitemap: {PUBLIC_BASE_URL}/sitemap.xml", ""]
    return PlainTextResponse("\n".join(lines))


@router.get("/sitemap.xml")
def sitemap() -> Response:
    paths = ["/", "/recaps"] + [f"/recaps/{slug}" for slug in published_slugs()]
    urls = "".join(
        f"  <url><loc>{PUBLIC_BASE_URL}{path}</loc></url>\n" for path in paths
    )
    body = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f"{urls}"
        "</urlset>\n"
    )
    return Response(content=body, media_type="application/xml")
