"""
Layer 3 — access request lifecycle integration tests.

Covers POST /request-access, GET /admin/access-requests,
POST /admin/access-requests/{id}/approve, POST /admin/access-requests/{id}/deny.

Each test uses a pre-assigned email from the module-scoped `req_emails` fixture.
Module teardown deletes all access_requests and authorized_users rows whose
email_hash matches any of the test emails — covering rows inserted both by the
API routes and by seed_user_committed.
"""
import hashlib
import os

import psycopg2
import pytest

_DEFAULT_URL = "postgresql://cartracker:cartracker@localhost:5432/cartracker"
_SALT = "test-salt"


def _hash(email: str) -> str:
    return hashlib.sha256((_SALT + email.lower()).encode()).hexdigest()


def _get_conn():
    from urllib.parse import urlparse
    url = os.environ.get("TEST_DATABASE_URL", _DEFAULT_URL)
    p = urlparse(url)
    return psycopg2.connect(
        host=p.hostname, port=p.port or 5432,
        dbname=p.path.lstrip("/"), user=p.username, password=p.password,
    )


@pytest.fixture(scope="module")
def req_emails(test_key_prefix):
    """Pre-defined test emails for this module — one per test scenario."""
    p = test_key_prefix
    return {
        "requester": f"{p}requester@test.local",
        "dup":       f"{p}dup@test.local",
        "preauth":   f"{p}preauth@test.local",
        "badrole":   f"{p}badrole@test.local",
        "approve":   f"{p}approve@test.local",
        "conflict":  f"{p}conflict@test.local",
        "deny":      f"{p}deny@test.local",
        "list":      f"{p}list@test.local",
    }


@pytest.fixture(autouse=True, scope="module")
def cleanup_access_data(req_emails):
    yield
    conn = _get_conn()
    conn.autocommit = True
    hashes = [_hash(e) for e in req_emails.values()]
    with conn.cursor() as cur:
        for h in hashes:
            cur.execute("DELETE FROM access_requests WHERE email_hash = %s", (h,))
            cur.execute("DELETE FROM authorized_users WHERE email_hash = %s", (h,))
    conn.close()


# ---------------------------------------------------------------------------
# Submit access request
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_submit_access_request_creates_db_row(api_client, verify_cur, req_emails):
    email = req_emails["requester"]

    response = api_client.post(
        "/request-access",
        data={"display_name": "Test Requester", "requested_role": "viewer"},
        headers={"X-Auth-Request-Email": email},
        follow_redirects=False,
    )

    assert response.status_code == 200

    verify_cur.execute(
        "SELECT status, requested_role, display_name FROM access_requests"
        " WHERE email_hash = %s",
        (_hash(email),),
    )
    row = verify_cur.fetchone()
    assert row is not None
    assert row["status"] == "pending"
    assert row["requested_role"] == "viewer"
    assert row["display_name"] == "Test Requester"


@pytest.mark.integration
def test_submit_access_request_duplicate_shows_pending(api_client, verify_cur, req_emails):
    email = req_emails["dup"]
    data = {"display_name": "Dup User", "requested_role": "viewer"}
    headers = {"X-Auth-Request-Email": email}

    api_client.post("/request-access", data=data, headers=headers, follow_redirects=False)
    response = api_client.post(
        "/request-access", data=data, headers=headers, follow_redirects=False
    )

    assert response.status_code == 200

    verify_cur.execute(
        "SELECT COUNT(*) FROM access_requests WHERE email_hash = %s",
        (_hash(email),),
    )
    assert verify_cur.fetchone()["count"] == 1


@pytest.mark.integration
def test_submit_access_request_already_authorized_redirects(
    api_client, seed_user_committed, req_emails
):
    email = req_emails["preauth"]
    seed_user_committed(_hash(email), "viewer")

    response = api_client.post(
        "/request-access",
        data={"display_name": "Already Auth", "requested_role": "viewer"},
        headers={"X-Auth-Request-Email": email},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/dashboard"


@pytest.mark.integration
def test_submit_access_request_invalid_role_returns_400(
    api_client, verify_cur, req_emails
):
    email = req_emails["badrole"]

    response = api_client.post(
        "/request-access",
        data={"display_name": "Bad Role", "requested_role": "superadmin"},
        headers={"X-Auth-Request-Email": email},
        follow_redirects=False,
    )

    assert response.status_code == 400

    verify_cur.execute(
        "SELECT 1 FROM access_requests WHERE email_hash = %s",
        (_hash(email),),
    )
    assert verify_cur.fetchone() is None


# ---------------------------------------------------------------------------
# Approve
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_approve_access_request_creates_authorized_user(
    api_client, verify_cur, req_emails
):
    email = req_emails["approve"]
    api_client.post(
        "/request-access",
        data={"display_name": "Approve Me", "requested_role": "observer"},
        headers={"X-Auth-Request-Email": email},
        follow_redirects=False,
    )
    verify_cur.execute(
        "SELECT id FROM access_requests WHERE email_hash = %s AND status = 'pending'",
        (_hash(email),),
    )
    req_id = verify_cur.fetchone()["id"]

    response = api_client.post(
        f"/admin/access-requests/{req_id}/approve", follow_redirects=False
    )
    assert response.status_code == 303

    verify_cur.execute(
        "SELECT role FROM authorized_users WHERE email_hash = %s",
        (_hash(email),),
    )
    user_row = verify_cur.fetchone()
    assert user_row is not None
    assert user_row["role"] == "observer"

    verify_cur.execute(
        "SELECT status, resolved_at FROM access_requests WHERE id = %s",
        (req_id,),
    )
    req_row = verify_cur.fetchone()
    assert req_row["status"] == "approved"
    assert req_row["resolved_at"] is not None


@pytest.mark.integration
def test_approve_access_request_conflict_upserts(
    api_client, verify_cur, seed_user_committed, req_emails
):
    """ON CONFLICT (email_hash) DO UPDATE fires when user already exists."""
    email = req_emails["conflict"]

    # Submit the access request first — the route redirects if the user already
    # exists in authorized_users, so we must create the request before seeding.
    api_client.post(
        "/request-access",
        data={"display_name": "Conflict User", "requested_role": "power_user"},
        headers={"X-Auth-Request-Email": email},
        follow_redirects=False,
    )

    # Now seed the existing user so the ON CONFLICT path fires on approve.
    seed_user_committed(_hash(email), "viewer")
    verify_cur.execute(
        "SELECT id FROM access_requests WHERE email_hash = %s AND status = 'pending'",
        (_hash(email),),
    )
    req_id = verify_cur.fetchone()["id"]

    api_client.post(
        f"/admin/access-requests/{req_id}/approve", follow_redirects=False
    )

    # Role should be updated to power_user, not duplicated
    verify_cur.execute(
        "SELECT role FROM authorized_users WHERE email_hash = %s",
        (_hash(email),),
    )
    rows = verify_cur.fetchall()
    assert len(rows) == 1
    assert rows[0]["role"] == "power_user"


# ---------------------------------------------------------------------------
# Deny
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_deny_access_request_updates_status(api_client, verify_cur, req_emails):
    email = req_emails["deny"]
    api_client.post(
        "/request-access",
        data={"display_name": "Deny Me", "requested_role": "viewer"},
        headers={"X-Auth-Request-Email": email},
        follow_redirects=False,
    )
    verify_cur.execute(
        "SELECT id FROM access_requests WHERE email_hash = %s AND status = 'pending'",
        (_hash(email),),
    )
    req_id = verify_cur.fetchone()["id"]

    response = api_client.post(
        f"/admin/access-requests/{req_id}/deny", follow_redirects=False
    )
    assert response.status_code == 303

    verify_cur.execute(
        "SELECT status FROM access_requests WHERE id = %s", (req_id,)
    )
    assert verify_cur.fetchone()["status"] == "denied"

    verify_cur.execute(
        "SELECT 1 FROM authorized_users WHERE email_hash = %s", (_hash(email),)
    )
    assert verify_cur.fetchone() is None


@pytest.mark.integration
def test_approve_nonexistent_request_redirects(api_client):
    response = api_client.post(
        "/admin/access-requests/99999/approve", follow_redirects=False
    )
    assert response.status_code == 303
    assert "/admin/access-requests" in response.headers["location"]


# ---------------------------------------------------------------------------
# List
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_list_access_requests_shows_pending(api_client, req_emails):
    email = req_emails["list"]
    api_client.post(
        "/request-access",
        data={"display_name": "L3 List User", "requested_role": "viewer"},
        headers={"X-Auth-Request-Email": email},
        follow_redirects=False,
    )

    response = api_client.get("/admin/access-requests")

    assert response.status_code == 200
    assert "L3 List User" in response.text
