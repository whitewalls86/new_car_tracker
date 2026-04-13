"""
Pipeline Ops — admin UI and deploy coordination for cartracker.
"""
import logging
import os
from logging.handlers import RotatingFileHandler

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, Response

from .routers.admin import router as admin_router
from .routers.auth import router as auth_router
from .routers.deploy import router as deploy_router
from .routers.info import router as info_router
from .routers.users import public_router as users_public_router
from .routers.users import router as users_router

_LOG_PATH = os.getenv("LOG_PATH", "/usr/app/logs/app.log")
os.makedirs(os.path.dirname(_LOG_PATH), exist_ok=True)
_log_handler = RotatingFileHandler(_LOG_PATH, maxBytes=5_000_000, backupCount=3)
_log_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
logging.getLogger().addHandler(_log_handler)
logging.getLogger().setLevel(logging.INFO)

app = FastAPI()
app.include_router(info_router)
app.include_router(auth_router)
app.include_router(deploy_router)
app.include_router(admin_router, prefix="/admin")
app.include_router(users_router, prefix="/admin")
# Public access-request routes — mounted at root so Caddy can reach them
# for authenticated-but-unauthorised users (no /admin prefix).
app.include_router(users_public_router)


# Observer middleware: block mutations for users with observer role.
# Caddy forwards X-User-Role on every request.
_MUTATING_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
_OBSERVER_EXEMPT_PATHS = {"/auth/check", "/health"}


@app.middleware("http")
async def observer_readonly(request: Request, call_next) -> Response:
    role = request.headers.get("x-user-role", "")
    if role == "observer" and request.method in _MUTATING_METHODS:
        if request.url.path not in _OBSERVER_EXEMPT_PATHS:
            return Response(status_code=403, content="Observers cannot make changes.")
    return await call_next(request)


@app.get("/")
@app.get("/admin")
def root():
    return RedirectResponse(url="/admin/searches/")


@app.get("/health")
def health():
    return {"ok": True}