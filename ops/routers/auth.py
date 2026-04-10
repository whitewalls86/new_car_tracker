"""
Auth check endpoint — internal only, called by Caddy forward_auth.
Never exposed through a public Caddy route.
"""
import hashlib
import os

from fastapi import APIRouter, Header
from fastapi.responses import Response

from shared.db import db_cursor

router = APIRouter()

_SALT = os.environ.get("AUTH_EMAIL_SALT", "")


def _hash_email(email: str) -> str:
    return hashlib.sha256((_SALT + email.lower()).encode()).hexdigest()


@router.get("/auth/check")
def auth_check(x_auth_request_email: str | None = Header(default=None)):
    """
    Called by Caddy forward_auth on every protected request.
    Returns 200 + X-User-Role header if the email is authorised, 403 otherwise.
    The email arrives via the X-Auth-Request-Email header set by oauth2-proxy.
    FastAPI automatically maps header names to snake_case parameters.
    """
    if not x_auth_request_email:
        return Response(status_code=403)

    email_hash = _hash_email(x_auth_request_email)

    try:
        with db_cursor(error_context="Auth-Check", dict_cursor=True) as cur:
            cur.execute(
                "SELECT role FROM authorized_users WHERE email_hash = %s",
                (email_hash,),
            )
            row = cur.fetchone()
    except Exception:
        return Response(status_code=503)

    if not row:
        return Response(status_code=403)

    return Response(
        status_code=200,
        headers={"X-User-Role": row["role"]},
    )
