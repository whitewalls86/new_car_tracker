# =============================================================================
# Tests for ops/routers/auth.py — /auth/check endpoint
# =============================================================================
import hashlib

from ops.routers.auth import _hash_email

SALT = "test-salt"


# ---------------------------------------------------------------------------
# Unit: _hash_email
# ---------------------------------------------------------------------------

def test_hash_email_lowercase(monkeypatch):
    monkeypatch.setattr("ops.routers.auth._SALT", SALT)
    assert _hash_email("Alice@Gmail.COM") == _hash_email("alice@gmail.com")


def test_hash_email_deterministic(monkeypatch):
    monkeypatch.setattr("ops.routers.auth._SALT", SALT)
    expected = hashlib.sha256((SALT + "alice@gmail.com").encode()).hexdigest()
    assert _hash_email("alice@gmail.com") == expected


# ---------------------------------------------------------------------------
# Integration: GET /auth/check
# ---------------------------------------------------------------------------

def test_auth_check_no_email(mock_client):
    resp = mock_client.get("/auth/check")
    assert resp.status_code == 403


def test_auth_check_unknown_email(mock_client, mock_cursor_context, monkeypatch):
    monkeypatch.setattr("ops.routers.auth._SALT", SALT)
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = None

    resp = mock_client.get(
        "/auth/check",
        headers={"X-Auth-Request-Email": "stranger@gmail.com"},
    )
    assert resp.status_code == 403


def test_auth_check_known_admin(mock_client, mock_cursor_context, monkeypatch):
    monkeypatch.setattr("ops.routers.auth._SALT", SALT)
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = {"role": "admin"}

    resp = mock_client.get(
        "/auth/check",
        headers={"X-Auth-Request-Email": "admin@gmail.com"},
    )
    assert resp.status_code == 200
    assert resp.headers["x-user-role"] == "admin"


def test_auth_check_known_observer(mock_client, mock_cursor_context, monkeypatch):
    monkeypatch.setattr("ops.routers.auth._SALT", SALT)
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = {"role": "observer"}

    resp = mock_client.get(
        "/auth/check",
        headers={"X-Auth-Request-Email": "observer@gmail.com"},
    )
    assert resp.status_code == 200
    assert resp.headers["x-user-role"] == "observer"


def test_auth_check_db_error(mock_client, mock_db_connection_error, mock_logger_error, monkeypatch):
    monkeypatch.setattr("ops.routers.auth._SALT", SALT)
    resp = mock_client.get(
        "/auth/check",
        headers={"X-Auth-Request-Email": "anyone@gmail.com"},
    )
    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# Role tier enforcement via ?require=
# ---------------------------------------------------------------------------

def test_auth_check_require_admin_allows_admin(mock_client, mock_cursor_context, monkeypatch):
    monkeypatch.setattr("ops.routers.auth._SALT", SALT)
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = {"role": "admin"}

    resp = mock_client.get(
        "/auth/check?require=admin",
        headers={"X-Auth-Request-Email": "admin@gmail.com"},
    )
    assert resp.status_code == 200
    assert resp.headers["x-user-role"] == "admin"


def test_auth_check_require_admin_rejects_power_user(mock_client, mock_cursor_context, monkeypatch):
    monkeypatch.setattr("ops.routers.auth._SALT", SALT)
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = {"role": "power_user"}

    resp = mock_client.get(
        "/auth/check?require=admin",
        headers={"X-Auth-Request-Email": "power@gmail.com"},
    )
    assert resp.status_code == 403


def test_auth_check_require_observer_allows_power_user(
    mock_client, mock_cursor_context, monkeypatch
):
    monkeypatch.setattr("ops.routers.auth._SALT", SALT)
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = {"role": "power_user"}

    resp = mock_client.get(
        "/auth/check?require=observer",
        headers={"X-Auth-Request-Email": "power@gmail.com"},
    )
    assert resp.status_code == 200


def test_auth_check_require_observer_rejects_viewer(mock_client, mock_cursor_context, monkeypatch):
    monkeypatch.setattr("ops.routers.auth._SALT", SALT)
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = {"role": "viewer"}

    resp = mock_client.get(
        "/auth/check?require=observer",
        headers={"X-Auth-Request-Email": "viewer@gmail.com"},
    )
    assert resp.status_code == 403


def test_auth_check_no_require_allows_any_role(mock_client, mock_cursor_context, monkeypatch):
    monkeypatch.setattr("ops.routers.auth._SALT", SALT)
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = {"role": "viewer"}

    resp = mock_client.get(
        "/auth/check",
        headers={"X-Auth-Request-Email": "viewer@gmail.com"},
    )
    assert resp.status_code == 200
    assert resp.headers["x-user-role"] == "viewer"
