"""
Pipeline Ops — admin UI and deploy coordination for cartracker.
"""

import os
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, generate_latest
from prometheus_fastapi_instrumentator import Instrumentator

from shared.api_models import HealthResponse
from shared.db_vocabularies import UserRole
from shared.logging_setup import configure_logging

from .coordination_metrics import COORDINATION_COLLECTOR
from .public_stats import public_stats_cache
from .routers.admin import router as admin_router
from .routers.auth import router as auth_router
from .routers.coordination import router as coordination_router
from .routers.deploy import router as deploy_router
from .routers.info import router as info_router
from .routers.maintenance import router as maintenance_router
from .routers.public import router as public_router
from .routers.scrape import router as scrape_router
from .routers.snapshots import router as snapshots_router
from .routers.users import public_router as users_public_router
from .routers.users import router as users_router

configure_logging()


def _public_stats_cache_loop(stop_event: threading.Event) -> None:
    public_stats_cache.refresh()
    while not stop_event.wait(60):
        public_stats_cache.refresh()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    stop_event = threading.Event()
    t = threading.Thread(target=_public_stats_cache_loop, args=(stop_event,), daemon=True)
    t.start()
    try:
        yield
    finally:
        stop_event.set()
        t.join(timeout=1)


app = FastAPI(lifespan=lifespan)
REGISTRY.register(COORDINATION_COLLECTOR)
Instrumentator().instrument(app)

# Plan 162 Stage AA: `/metrics` is declared here rather than by
# `Instrumentator.expose()`, which registers a closure from the library. That
# closure is not a function in this repository, so no rule that resolves a
# route to its handler could reach this route -- and a rule that cannot reach a
# route is a rule with a hole in it rather than a rule with an exception.
#
# Equivalent to what `expose()` served: its only other branch is the
# multiprocess registry, and `PROMETHEUS_MULTIPROC_DIR` is set nowhere in this
# repository and unset in all three running containers. `should_gzip` was left
# at its default of False. `instrument()` still does the collecting.
@app.get("/metrics", response_class=Response)
def metrics() -> Response:
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)


app.mount(
    "/static_ops",
    StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static_ops")),
    name="static_ops",
)
app.include_router(info_router)
app.include_router(public_router)
app.include_router(auth_router)
app.include_router(deploy_router)
app.include_router(scrape_router)
app.include_router(admin_router, prefix="/admin")
app.include_router(users_router, prefix="/admin")
app.include_router(snapshots_router)
# Public access-request routes — mounted at root so Caddy can reach them
# for authenticated-but-unauthorised users (no /admin prefix).
app.include_router(users_public_router)
app.include_router(maintenance_router)
app.include_router(coordination_router)


# Observer middleware: block mutations for users with observer role.
# Caddy forwards X-User-Role on every request.
_MUTATING_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
_OBSERVER_EXEMPT_PATHS = {"/auth/check", "/health"}


@app.middleware("http")
async def observer_readonly(request: Request, call_next) -> Response:
    role = request.headers.get("x-user-role", "")
    if role == UserRole.OBSERVER and request.method in _MUTATING_METHODS:
        if request.url.path not in _OBSERVER_EXEMPT_PATHS:
            return Response(status_code=403, content="Observers cannot make changes.")
    return await call_next(request)


# ``/`` belonged to this redirect until Plan 138 Stage 2, when it became the
# public landing page (``ops.routers.info``). Caddy never routed ``/`` here
# before that stage -- the catch-all sent it to Streamlit -- so nothing external
# was relying on the old behaviour. ``/admin`` keeps it.
@app.get(
    "/admin",
    response_class=RedirectResponse,
    responses={
        307: {"description": "Redirect to the admin landing page."},
    },
)
def root():
    return RedirectResponse(url="/admin/searches/")


@app.get("/health", response_model=HealthResponse)
def health():
    return {"ok": True}
