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

from ops.email import send_access_approved, send_access_denied
from ops.queries import (
    APPROVE_ACCESS_REQUEST,
    DELETE_AUTHORIZED_USER,
    DENY_ACCESS_REQUEST,
    INSERT_ACCESS_REQUEST,
    SELECT_ACCESS_REQUESTS,
    SELECT_AUTHORIZED_USERS,
    SELECT_PENDING_REQUEST_DETAILS,
    SELECT_PENDING_REQUEST_FOR_EMAIL,
    SELECT_PENDING_REQUEST_ID_FOR_EMAIL,
    SELECT_PENDING_REQUEST_NOTIFICATION_EMAIL,
    SELECT_USER_ROLE,
    UPDATE_USER_ROLE,
    UPSERT_AUTHORIZED_USER,
)
from shared.db import db_cursor
from shared.db_vocabularies import RequestableRole, UserRole

from .auth import _hash_email

logger = logging.getLogger("pipeline_ops")
router = APIRouter()        # admin routes (prefix /admin)
public_router = APIRouter()  # public routes (no prefix)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


def _db_error_response(request: Request):
    return templates.TemplateResponse(request=request, name="admin/error.html", context={
        "request": request,
        "message": "Database unavailable. Please try again later.",
    }, status_code=503)


def _not_found_response(request: Request, message: str):
    """The answer when a mutation matched no row.

    Plan 162 Stage Y: these routes redirected to the list page whether the
    target existed or not, so a revocation that revoked nobody was reported to
    the operator exactly like one that worked.
    """
    return templates.TemplateResponse(request=request, name="admin/error.html", context={
        "request": request,
        "heading": "Not Found",
        "message": message,
    }, status_code=404)

_TELEGRAM_API = os.environ.get("TELEGRAM_API", "")
_TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

ROLE_LABELS = {
    "admin": "Admin",
    "power_user": "Power User",
    "observer": "Observer",
    "viewer": "Viewer",
}

# The database owns these three: `access_requests.requested_role` carries a
# CHECK that omits `admin`, deliberately -- an admin is granted, never
# requested. Derived rather than retyped so the omission cannot drift.
#
# **Iteration order, not `sorted()`, because this is what the form renders.**
# The vocabulary declares itself least-privileged first, which is the order the
# request page has always offered; `sorted()` would put `observer` first on an
# alphabetical accident, and nothing asserts the order, so that regression
# would have shipped green. Taking the whole enum rather than listing members
# also means a fourth requestable role reaches the form on its own.
REQUESTABLE_ROLES = list(RequestableRole)


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
        response = http_requests.post(
            f"https://api.telegram.org/bot{_TELEGRAM_API}/sendMessage",
            json={"chat_id": _TELEGRAM_CHAT_ID, "text": msg},
            timeout=5,
        )
        # The status, before assuming it sent. Plan 162 Stage Y, G27. Telegram
        # answers 200 for a delivered message and 4xx for a bad chat id or a
        # revoked token, and without this an access request nobody was told
        # about was indistinguishable from one that reached the phone.
        response.raise_for_status()
    except Exception:
        logger.warning("Failed to send Telegram notification for access request")


# ---------------------------------------------------------------------------
# Public: request-access
# ---------------------------------------------------------------------------

def _redirect_for_role(role: str) -> RedirectResponse:
    if role in frozenset(UserRole) - {UserRole.VIEWER}:
        return RedirectResponse(url="/admin", status_code=303)
    return RedirectResponse(url="/dashboard", status_code=303)


@public_router.get(
    "/request-access",
    response_class=HTMLResponse,
    responses={
        303: {"description": "Already authorised; redirect to the role's landing page."},
    },
)
def request_access_form(request: Request):
    email = request.headers.get("x-auth-request-email", "")
    if email:
        email_hash = _hash_email(email)
        try:
            with db_cursor(error_context="Request-Access-Check", dict_cursor=True) as cur:
                cur.execute(SELECT_USER_ROLE, (email_hash,))
                row = cur.fetchone()
                if row:
                    return _redirect_for_role(row["role"])

                cur.execute(SELECT_PENDING_REQUEST_FOR_EMAIL, (email_hash,))
                pending = cur.fetchone()
        except Exception:
            pending = None
    else:
        pending = None

    return templates.TemplateResponse(request=request, name="request_access.html", context={
        "request": request,
        "roles": REQUESTABLE_ROLES,
        "error": None,
        "submitted": False,
        "pending": pending,
    })


@public_router.post(
    "/request-access",
    response_class=HTMLResponse,
    responses={
        303: {"description": "Already authorised, or a request is already pending."},
        400: {"description": "No email could be determined, or the role is not requestable."},
        503: {"description": "Database unavailable."},
    },
)
def submit_access_request(
    request: Request,
    display_name: str = Form(...),
    requested_role: str = Form(...),
    notify_email: str = Form(default=None),
):
    email = request.headers.get("x-auth-request-email", "")
    if not email:
        return templates.TemplateResponse(request=request, name="request_access.html", context={
            "request": request,
            "roles": REQUESTABLE_ROLES,
            "error": "Could not determine your email. Please try signing in again.",
            "submitted": False,
            "pending": None,
        }, status_code=400)

    if requested_role not in REQUESTABLE_ROLES:
        return templates.TemplateResponse(request=request, name="request_access.html", context={
            "request": request,
            "roles": REQUESTABLE_ROLES,
            "error": "Invalid role selected.",
            "submitted": False,
            "pending": None,
        }, status_code=400)

    email_hash = _hash_email(email)

    try:
        with db_cursor(error_context="Submit-Access-Request", dict_cursor=True) as cur:
            # Redirect if already authorised
            cur.execute(SELECT_USER_ROLE, (email_hash,))
            existing = cur.fetchone()
            if existing:
                return _redirect_for_role(existing["role"])

            # Guard against duplicate pending requests
            cur.execute(SELECT_PENDING_REQUEST_ID_FOR_EMAIL, (email_hash,))
            if cur.fetchone():
                return templates.TemplateResponse(
                    request=request,
                    name="request_access.html",
                    context={
                        "request": request,
                        "roles": REQUESTABLE_ROLES,
                        "error": None,
                        "submitted": False,
                        "pending": {"status": "pending"},
                    },
                )

            stored_email = email if notify_email == "on" else None
            cur.execute(
                INSERT_ACCESS_REQUEST,
                (email_hash, requested_role, display_name.strip() or None, stored_email),
            )
    except Exception:
        logger.exception("Failed to insert access request")
        return templates.TemplateResponse(request=request, name="request_access.html", context={
            "request": request,
            "roles": REQUESTABLE_ROLES,
            "error": "Database error. Please try again later.",
            "submitted": False,
            "pending": None,
        }, status_code=503)

    _notify_access_request(email_hash, requested_role)

    return templates.TemplateResponse(request=request, name="request_access.html", context={
        "request": request,
        "roles": REQUESTABLE_ROLES,
        "error": None,
        "submitted": True,
        "pending": None,
    })


# ---------------------------------------------------------------------------
# Admin: user management
# ---------------------------------------------------------------------------

@router.get("/users", response_class=HTMLResponse)
def list_users(request: Request):
    try:
        with db_cursor(error_context="List-Users", dict_cursor=True) as cur:
            cur.execute(SELECT_AUTHORIZED_USERS)
            users = cur.fetchall()
    except Exception:
        users = []

    return templates.TemplateResponse(request=request, name="admin/users.html", context={
        "request": request,
        "users": users,
        "roles": list(ROLE_LABELS.keys()),
        "role_labels": ROLE_LABELS,
    })


@router.post(
    "/users/{user_id}/role",
    response_class=HTMLResponse,
    # Plan 162 Stage AA: the success path redirects, so 303 is the
    # declared default rather than the 200 FastAPI would otherwise put in
    # the contract -- a code no exit of this handler produces.
    status_code=303,
    responses={
        303: {"description": "Role changed; redirect to the user list."},
        400: {"description": "Not a role this service defines."},
        404: {"description": "No user with that id; nothing was changed."},
        503: {"description": "Database unavailable."},
    },
)
def change_user_role(
    request: Request,
    user_id: int,
    role: str = Form(...),
):
    if role not in ROLE_LABELS:
        return templates.TemplateResponse(request=request, name="admin/error.html", context={
            "request": request,
            "heading": "Bad Request",
            "message": f"{role!r} is not a role.",
        }, status_code=400)
    try:
        with db_cursor(error_context="Change-User-Role") as cur:
            cur.execute(UPDATE_USER_ROLE, (role, user_id))
            matched = cur.rowcount
    except Exception:
        logger.exception("Failed to update user role")
        return _db_error_response(request=request)
    if not matched:
        return _not_found_response(request, f"No user with id {user_id}.")
    return RedirectResponse(url="/admin/users", status_code=303)


@router.post(
    "/users/{user_id}/revoke",
    response_class=HTMLResponse,
    # Plan 162 Stage AA: the success path redirects, so 303 is the
    # declared default rather than the 200 FastAPI would otherwise put in
    # the contract -- a code no exit of this handler produces.
    status_code=303,
    responses={
        303: {"description": "User revoked; redirect to the user list."},
        404: {"description": "No user with that id; nobody was revoked."},
        503: {"description": "Database unavailable."},
    },
)
def revoke_user(request: Request, user_id: int):
    try:
        with db_cursor(error_context="Revoke-User") as cur:
            cur.execute(DELETE_AUTHORIZED_USER, (user_id,))
            matched = cur.rowcount
    except Exception:
        logger.exception("Failed to revoke user")
        return _db_error_response(request=request)
    if not matched:
        return _not_found_response(request, f"No user with id {user_id}.")
    return RedirectResponse(url="/admin/users", status_code=303)


# ---------------------------------------------------------------------------
# Admin: access requests
# ---------------------------------------------------------------------------

@router.get("/access-requests", response_class=HTMLResponse)
def list_access_requests(request: Request):
    try:
        with db_cursor(error_context="List-Access-Requests", dict_cursor=True) as cur:
            cur.execute(SELECT_ACCESS_REQUESTS)
            requests_ = cur.fetchall()
    except Exception:
        requests_ = []

    return templates.TemplateResponse(request=request, name="admin/access_requests.html", context={
        "request": request,
        "access_requests": requests_,
        "role_labels": ROLE_LABELS,
    })


@router.post(
    "/access-requests/{req_id}/approve",
    response_class=HTMLResponse,
    # Plan 162 Stage AA: the success path redirects, so 303 is the
    # declared default rather than the 200 FastAPI would otherwise put in
    # the contract -- a code no exit of this handler produces.
    status_code=303,
    responses={
        303: {"description": "Request approved; redirect to the request list."},
        404: {"description": "No pending request with that id."},
        503: {"description": "Database unavailable."},
    },
)
def approve_access_request(
    request: Request,
    req_id: int,
):
    admin_email = request.headers.get("x-auth-request-email", "")
    admin_hash = _hash_email(admin_email) if admin_email else None

    try:
        with db_cursor(error_context="Approve-Access-Request", dict_cursor=True) as cur:
            cur.execute(SELECT_PENDING_REQUEST_DETAILS, (req_id,))
            row = cur.fetchone()
            if not row:
                return _not_found_response(request, f"No pending request with id {req_id}.")

            cur.execute(
                UPSERT_AUTHORIZED_USER,
                (row["email_hash"], row["requested_role"], row["display_name"], admin_hash),
            )
            cur.execute(APPROVE_ACCESS_REQUEST, (admin_hash, req_id))
    except Exception:
        logger.exception("Failed to approve access request")
        return _db_error_response(request=request)

    if row and row.get("notification_email"):
        send_access_approved(row["notification_email"], row["requested_role"])

    return RedirectResponse(url="/admin/access-requests", status_code=303)


@router.post(
    "/access-requests/{req_id}/deny",
    response_class=HTMLResponse,
    # Plan 162 Stage AA: the success path redirects, so 303 is the
    # declared default rather than the 200 FastAPI would otherwise put in
    # the contract -- a code no exit of this handler produces.
    status_code=303,
    responses={
        303: {"description": "Request denied; redirect to the request list."},
        404: {"description": "No pending request with that id."},
        503: {"description": "Database unavailable."},
    },
)
def deny_access_request(request: Request, req_id: int):
    admin_email = request.headers.get("x-auth-request-email", "")
    admin_hash = _hash_email(admin_email) if admin_email else None
    notification_email = None

    try:
        with db_cursor(error_context="Deny-Access-Request", dict_cursor=True) as cur:
            cur.execute(SELECT_PENDING_REQUEST_NOTIFICATION_EMAIL, (req_id,))
            row = cur.fetchone()
            if not row:
                return _not_found_response(request, f"No pending request with id {req_id}.")
            notification_email = row.get("notification_email")
            cur.execute(DENY_ACCESS_REQUEST, (admin_hash, req_id))
    except Exception:
        logger.exception("Failed to deny access request")
        return _db_error_response(request=request)

    if notification_email:
        send_access_denied(notification_email)

    return RedirectResponse(url="/admin/access-requests", status_code=303)
