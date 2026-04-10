"""
User management and access-request routes.
  GET  /request-access          — public form for users who got 403
  POST /request-access          — submit access request
  GET  /admin/users             — list authorised users (admin only at Caddy tier)
  POST /admin/users/{id}/role   — change a user's role
  POST /admin/users/{id}/revoke — remove a user
  GET  /admin/access-requests   — list pending requests
  POST /admin/access-requests/{id}/approve
  POST /admin/access-requests/{id}/deny
"""
import logging
import os

import requests as http_requests
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from shared.db import db_cursor

from .auth import _hash_email

logger = logging.getLogger("pipeline_ops")
router = APIRouter()        # admin routes (prefix /admin)
public_router = APIRouter()  # public routes (no prefix)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

_TELEGRAM_API = os.environ.get("TELEGRAM_API", "")
_TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

ROLE_LABELS = {
    "admin": "Admin",
    "power_user": "Power User",
    "observer": "Observer",
    "viewer": "Viewer",
}

REQUESTABLE_ROLES = ["viewer", "observer", "power_user"]


def _notify_access_request(email_hash: str, requested_role: str) -> None:
    if not _TELEGRAM_API or not _TELEGRAM_CHAT_ID:
        return
    try:
        msg = (
            f"New access request\n"
            f"Role: {requested_role}\n"
            f"Email hash: {email_hash[:12]}…\n"
            f"Approve at: https://cartracker.info/admin/access-requests"
        )
        http_requests.post(
            f"https://api.telegram.org/bot{_TELEGRAM_API}/sendMessage",
            json={"chat_id": _TELEGRAM_CHAT_ID, "text": msg},
            timeout=5,
        )
    except Exception:
        logger.warning("Failed to send Telegram notification for access request")


# ---------------------------------------------------------------------------
# Public: request-access
# ---------------------------------------------------------------------------

@public_router.get("/request-access", response_class=HTMLResponse)
def request_access_form(request: Request):
    return templates.TemplateResponse(request=request, name="request_access.html", context={
        "request": request,
        "roles": REQUESTABLE_ROLES,
        "error": None,
        "submitted": False,
    })


@public_router.post("/request-access", response_class=HTMLResponse)
def submit_access_request(
    request: Request,
    display_name: str = Form(...),
    requested_role: str = Form(...),
):
    email = request.headers.get("x-auth-request-email", "")
    if not email:
        return templates.TemplateResponse(request=request, name="request_access.html", context={
            "request": request,
            "roles": REQUESTABLE_ROLES,
            "error": "Could not determine your email. Please try signing in again.",
            "submitted": False,
        }, status_code=400)

    if requested_role not in REQUESTABLE_ROLES:
        return templates.TemplateResponse(request=request, name="request_access.html", context={
            "request": request,
            "roles": REQUESTABLE_ROLES,
            "error": "Invalid role selected.",
            "submitted": False,
        }, status_code=400)

    email_hash = _hash_email(email)

    try:
        with db_cursor(error_context="Submit-Access-Request", dict_cursor=True) as cur:
            cur.execute(
                """INSERT INTO access_requests (email_hash, requested_role, display_name)
                   VALUES (%s, %s, %s)""",
                (email_hash, requested_role, display_name.strip() or None),
            )
    except Exception:
        logger.exception("Failed to insert access request")
        return templates.TemplateResponse(request=request, name="request_access.html", context={
            "request": request,
            "roles": REQUESTABLE_ROLES,
            "error": "Database error. Please try again later.",
            "submitted": False,
        }, status_code=503)

    _notify_access_request(email_hash, requested_role)

    return templates.TemplateResponse(request=request, name="request_access.html", context={
        "request": request,
        "roles": REQUESTABLE_ROLES,
        "error": None,
        "submitted": True,
    })


# ---------------------------------------------------------------------------
# Admin: user management
# ---------------------------------------------------------------------------

@router.get("/users", response_class=HTMLResponse)
def list_users(request: Request):
    try:
        with db_cursor(error_context="List-Users", dict_cursor=True) as cur:
            cur.execute(
                """SELECT id, email_hash, role, display_name, created_at
                   FROM authorized_users
                   ORDER BY role, created_at"""
            )
            users = cur.fetchall()
    except Exception:
        users = []

    return templates.TemplateResponse(request=request, name="admin/users.html", context={
        "request": request,
        "users": users,
        "roles": list(ROLE_LABELS.keys()),
        "role_labels": ROLE_LABELS,
    })


@router.post("/users/{user_id}/role", response_class=HTMLResponse)
def change_user_role(
    request: Request,
    user_id: int,
    role: str = Form(...),
):
    if role not in ROLE_LABELS:
        return RedirectResponse(url="/admin/users", status_code=303)
    try:
        with db_cursor(error_context="Change-User-Role") as cur:
            cur.execute(
                "UPDATE authorized_users SET role = %s WHERE id = %s",
                (role, user_id),
            )
    except Exception:
        logger.exception("Failed to update user role")
    return RedirectResponse(url="/admin/users", status_code=303)


@router.post("/users/{user_id}/revoke", response_class=HTMLResponse)
def revoke_user(request: Request, user_id: int):
    try:
        with db_cursor(error_context="Revoke-User") as cur:
            cur.execute("DELETE FROM authorized_users WHERE id = %s", (user_id,))
    except Exception:
        logger.exception("Failed to revoke user")
    return RedirectResponse(url="/admin/users", status_code=303)


# ---------------------------------------------------------------------------
# Admin: access requests
# ---------------------------------------------------------------------------

@router.get("/access-requests", response_class=HTMLResponse)
def list_access_requests(request: Request):
    try:
        with db_cursor(error_context="List-Access-Requests", dict_cursor=True) as cur:
            cur.execute(
                """SELECT id, email_hash, display_name, requested_role, requested_at, status,
                          resolved_at, resolved_by
                   FROM access_requests
                   ORDER BY
                     CASE status WHEN 'pending' THEN 0 ELSE 1 END,
                     requested_at DESC"""
            )
            requests_ = cur.fetchall()
    except Exception:
        requests_ = []

    return templates.TemplateResponse(request=request, name="admin/access_requests.html", context={
        "request": request,
        "access_requests": requests_,
        "role_labels": ROLE_LABELS,
    })


@router.post("/access-requests/{req_id}/approve", response_class=HTMLResponse)
def approve_access_request(
    request: Request,
    req_id: int,
):
    admin_email = request.headers.get("x-auth-request-email", "")
    admin_hash = _hash_email(admin_email) if admin_email else None

    try:
        with db_cursor(error_context="Approve-Access-Request", dict_cursor=True) as cur:
            cur.execute(
                """SELECT email_hash, requested_role, display_name
                   FROM access_requests WHERE id = %s AND status = 'pending'""",
                (req_id,),
            )
            row = cur.fetchone()
            if not row:
                return RedirectResponse(url="/admin/access-requests", status_code=303)

            cur.execute(
                """INSERT INTO authorized_users
                       (email_hash, role, display_name, created_by)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (email_hash) DO UPDATE
                       SET role = EXCLUDED.role,
                           created_by = EXCLUDED.created_by""",
                (row["email_hash"], row["requested_role"], row["display_name"], admin_hash),
            )
            cur.execute(
                """UPDATE access_requests
                   SET status = 'approved', resolved_at = now(), resolved_by = %s
                   WHERE id = %s""",
                (admin_hash, req_id),
            )
    except Exception:
        logger.exception("Failed to approve access request")

    return RedirectResponse(url="/admin/access-requests", status_code=303)


@router.post("/access-requests/{req_id}/deny", response_class=HTMLResponse)
def deny_access_request(request: Request, req_id: int):
    admin_email = request.headers.get("x-auth-request-email", "")
    admin_hash = _hash_email(admin_email) if admin_email else None

    try:
        with db_cursor(error_context="Deny-Access-Request") as cur:
            cur.execute(
                """UPDATE access_requests
                   SET status = 'denied', resolved_at = now(), resolved_by = %s
                   WHERE id = %s AND status = 'pending'""",
                (admin_hash, req_id),
            )
    except Exception:
        logger.exception("Failed to deny access request")

    return RedirectResponse(url="/admin/access-requests", status_code=303)
