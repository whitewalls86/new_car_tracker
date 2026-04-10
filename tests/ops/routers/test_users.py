# =============================================================================
# Tests for ops/routers/users.py — user management and access requests
# =============================================================================
from datetime import datetime, timezone

SALT = "test-salt"


# ---------------------------------------------------------------------------
# GET /request-access (public)
# ---------------------------------------------------------------------------

def test_request_access_form_renders(mock_client):
    resp = mock_client.get("/request-access")
    assert resp.status_code == 200
    assert "Request Access" in resp.text


# ---------------------------------------------------------------------------
# POST /request-access
# ---------------------------------------------------------------------------

def test_submit_access_request_no_email(mock_client):
    resp = mock_client.post(
        "/request-access",
        data={"display_name": "Test User", "requested_role": "viewer"},
    )
    assert resp.status_code == 400
    assert "Could not determine your email" in resp.text


def test_submit_access_request_invalid_role(mock_client):
    resp = mock_client.post(
        "/request-access",
        data={"display_name": "Test User", "requested_role": "admin"},
        headers={"X-Auth-Request-Email": "user@gmail.com"},
    )
    assert resp.status_code == 400
    assert "Invalid role" in resp.text


def test_submit_access_request_ok(mock_client, mock_cursor_context, monkeypatch):
    monkeypatch.setattr("ops.routers.auth._SALT", SALT)
    resp = mock_client.post(
        "/request-access",
        data={"display_name": "Test User", "requested_role": "viewer"},
        headers={"X-Auth-Request-Email": "user@gmail.com"},
    )
    assert resp.status_code == 200
    assert "request has been submitted" in resp.text

    _, cursor = mock_cursor_context
    cursor.execute.assert_called_once()
    sql = cursor.execute.call_args[0][0]
    assert "INSERT INTO access_requests" in sql


def test_submit_access_request_db_error(
    mock_client, mock_db_connection_error, mock_logger_error, monkeypatch,
):
    monkeypatch.setattr("ops.routers.auth._SALT", SALT)
    resp = mock_client.post(
        "/request-access",
        data={"display_name": "Test User", "requested_role": "observer"},
        headers={"X-Auth-Request-Email": "user@gmail.com"},
    )
    assert resp.status_code == 503
    assert "Database error" in resp.text


def test_submit_access_request_sends_telegram(
    mock_client, mock_cursor_context, monkeypatch, mocker,
):
    monkeypatch.setattr("ops.routers.auth._SALT", SALT)
    monkeypatch.setattr("ops.routers.users._TELEGRAM_API", "fake-token")
    monkeypatch.setattr("ops.routers.users._TELEGRAM_CHAT_ID", "12345")
    mock_post = mocker.patch("ops.routers.users.http_requests.post")

    resp = mock_client.post(
        "/request-access",
        data={"display_name": "Test User", "requested_role": "viewer"},
        headers={"X-Auth-Request-Email": "user@gmail.com"},
    )
    assert resp.status_code == 200
    mock_post.assert_called_once()
    call_url = mock_post.call_args[0][0]
    assert "sendMessage" in call_url
    assert "fake-token" in call_url


def test_submit_access_request_no_telegram_when_unconfigured(
    mock_client, mock_cursor_context, monkeypatch, mocker,
):
    monkeypatch.setattr("ops.routers.auth._SALT", SALT)
    monkeypatch.setattr("ops.routers.users._TELEGRAM_API", "")
    monkeypatch.setattr("ops.routers.users._TELEGRAM_CHAT_ID", "")
    mock_post = mocker.patch("ops.routers.users.http_requests.post")

    mock_client.post(
        "/request-access",
        data={"display_name": "Test User", "requested_role": "viewer"},
        headers={"X-Auth-Request-Email": "user@gmail.com"},
    )
    mock_post.assert_not_called()


# ---------------------------------------------------------------------------
# GET /admin/users
# ---------------------------------------------------------------------------

def test_list_users_ok(mock_client, mock_cursor_context):
    _, cursor = mock_cursor_context
    cursor.fetchall.return_value = [
        {
            "id": 1,
            "email_hash": "abc123def456",
            "role": "admin",
            "display_name": "Admin User",
            "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        },
    ]
    resp = mock_client.get("/admin/users")
    assert resp.status_code == 200
    assert "Admin User" in resp.text
    assert "abc123def456" in resp.text


def test_list_users_db_error(mock_client, mock_db_connection_error, mock_logger_error):
    resp = mock_client.get("/admin/users")
    assert resp.status_code == 200
    assert "No users found" in resp.text


# ---------------------------------------------------------------------------
# POST /admin/users/{id}/role
# ---------------------------------------------------------------------------

def test_change_user_role_ok(mock_client, mock_cursor_context, monkeypatch):
    monkeypatch.setattr("ops.routers.auth._SALT", SALT)
    resp = mock_client.post(
        "/admin/users/1/role",
        data={"role": "observer"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert resp.headers["location"] == "/admin/users"

    _, cursor = mock_cursor_context
    cursor.execute.assert_called_once()
    sql = cursor.execute.call_args[0][0]
    assert "UPDATE authorized_users SET role" in sql


def test_change_user_role_invalid(mock_client):
    resp = mock_client.post(
        "/admin/users/1/role",
        data={"role": "superadmin"},
        follow_redirects=False,
    )
    assert resp.status_code == 303


# ---------------------------------------------------------------------------
# POST /admin/users/{id}/revoke
# ---------------------------------------------------------------------------

def test_revoke_user_ok(mock_client, mock_cursor_context, monkeypatch):
    monkeypatch.setattr("ops.routers.auth._SALT", SALT)
    resp = mock_client.post(
        "/admin/users/1/revoke",
        follow_redirects=False,
    )
    assert resp.status_code == 303

    _, cursor = mock_cursor_context
    cursor.execute.assert_called_once()
    sql = cursor.execute.call_args[0][0]
    assert "DELETE FROM authorized_users" in sql


# ---------------------------------------------------------------------------
# GET /admin/access-requests
# ---------------------------------------------------------------------------

def test_list_access_requests_ok(mock_client, mock_cursor_context):
    _, cursor = mock_cursor_context
    cursor.fetchall.return_value = [
        {
            "id": 1,
            "email_hash": "abc123def456abc1",
            "display_name": "Jane Smith",
            "requested_role": "viewer",
            "requested_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "status": "pending",
            "resolved_at": None,
            "resolved_by": None,
        },
    ]
    resp = mock_client.get("/admin/access-requests")
    assert resp.status_code == 200
    assert "Jane Smith" in resp.text
    assert "Pending" in resp.text


def test_list_access_requests_empty(mock_client, mock_db_connection_error, mock_logger_error):
    resp = mock_client.get("/admin/access-requests")
    assert resp.status_code == 200
    assert "No access requests" in resp.text


# ---------------------------------------------------------------------------
# POST /admin/access-requests/{id}/approve
# ---------------------------------------------------------------------------

def test_approve_access_request_ok(mock_client, mock_cursor_context, monkeypatch):
    monkeypatch.setattr("ops.routers.auth._SALT", SALT)
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = {
        "email_hash": "abc123",
        "requested_role": "viewer",
        "display_name": "New User",
    }

    resp = mock_client.post(
        "/admin/access-requests/1/approve",
        headers={"X-Auth-Request-Email": "admin@gmail.com"},
        follow_redirects=False,
    )
    assert resp.status_code == 303

    # Should have: SELECT, INSERT into authorized_users, UPDATE access_requests
    assert cursor.execute.call_count == 3


def test_approve_access_request_not_found(mock_client, mock_cursor_context, monkeypatch):
    monkeypatch.setattr("ops.routers.auth._SALT", SALT)
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = None

    resp = mock_client.post(
        "/admin/access-requests/999/approve",
        follow_redirects=False,
    )
    assert resp.status_code == 303
    # Only the SELECT was called, no INSERT/UPDATE
    assert cursor.execute.call_count == 1


# ---------------------------------------------------------------------------
# POST /admin/access-requests/{id}/deny
# ---------------------------------------------------------------------------

def test_deny_access_request_ok(mock_client, mock_cursor_context, monkeypatch):
    monkeypatch.setattr("ops.routers.auth._SALT", SALT)
    resp = mock_client.post(
        "/admin/access-requests/1/deny",
        headers={"X-Auth-Request-Email": "admin@gmail.com"},
        follow_redirects=False,
    )
    assert resp.status_code == 303

    _, cursor = mock_cursor_context
    cursor.execute.assert_called_once()
    sql = cursor.execute.call_args[0][0]
    assert "status = 'denied'" in sql
