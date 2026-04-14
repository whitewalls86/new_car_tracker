"""
Tests for access request flow: GET/POST /request-access, email notifications.
"""


# ---------------------------------------------------------------------------
# GET /request-access
# ---------------------------------------------------------------------------

def test_get_request_access_no_email_shows_form(mock_client):
    """No X-Auth-Request-Email header → show form."""
    resp = mock_client.get("/request-access")
    assert resp.status_code == 200
    assert "Submit Request" in resp.text


def test_get_request_access_authorized_viewer_redirects_to_dashboard(
    mock_client, mock_cursor_context
):
    """Already-authorized viewer → redirect to /dashboard."""
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = {"role": "viewer"}
    resp = mock_client.get(
        "/request-access",
        headers={"X-Auth-Request-Email": "viewer@example.com"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert resp.headers["location"] == "/dashboard"


def test_get_request_access_authorized_admin_redirects_to_admin(
    mock_client, mock_cursor_context
):
    """Already-authorized admin → redirect to /admin."""
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = {"role": "admin"}
    resp = mock_client.get(
        "/request-access",
        headers={"X-Auth-Request-Email": "admin@example.com"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert resp.headers["location"] == "/admin"


def test_get_request_access_pending_request_shows_pending(
    mock_client, mock_cursor_context
):
    """Existing pending request → show pending state, not form."""
    _, cursor = mock_cursor_context
    # First fetchone: no authorized_users row. Second: pending request exists.
    cursor.fetchone.side_effect = [None, {"status": "pending"}]
    resp = mock_client.get(
        "/request-access",
        headers={"X-Auth-Request-Email": "user@example.com"},
    )
    assert resp.status_code == 200
    assert "pending" in resp.text
    assert "Submit Request" not in resp.text


# ---------------------------------------------------------------------------
# POST /request-access
# ---------------------------------------------------------------------------

def test_post_request_access_no_email_returns_400(mock_client):
    """No X-Auth-Request-Email → 400."""
    resp = mock_client.post(
        "/request-access",
        data={"display_name": "Test User", "requested_role": "viewer"},
    )
    assert resp.status_code == 400


def test_post_request_access_invalid_role_returns_400(mock_client):
    """Invalid role → 400."""
    resp = mock_client.post(
        "/request-access",
        data={"display_name": "Test User", "requested_role": "superuser"},
        headers={"X-Auth-Request-Email": "user@example.com"},
    )
    assert resp.status_code == 400


def test_post_request_access_already_authorized_redirects(
    mock_client, mock_cursor_context
):
    """Already-authorized user posting → redirect to appropriate destination."""
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = {"role": "viewer"}
    resp = mock_client.post(
        "/request-access",
        data={"display_name": "Test User", "requested_role": "viewer"},
        headers={"X-Auth-Request-Email": "user@example.com"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert resp.headers["location"] == "/dashboard"


def test_post_request_access_duplicate_pending_shows_pending(
    mock_client, mock_cursor_context
):
    """Duplicate pending submission → show pending state."""
    _, cursor = mock_cursor_context
    # First fetchone: not in authorized_users. Second: existing pending request.
    cursor.fetchone.side_effect = [None, {"id": 1}]
    resp = mock_client.post(
        "/request-access",
        data={"display_name": "Test User", "requested_role": "viewer"},
        headers={"X-Auth-Request-Email": "user@example.com"},
    )
    assert resp.status_code == 200
    assert "pending" in resp.text


def test_post_request_access_success_shows_submitted(
    mock_client, mock_cursor_context, mocker
):
    """Successful submission → show submitted confirmation."""
    _, cursor = mock_cursor_context
    cursor.fetchone.side_effect = [None, None]  # not authorized, no pending
    mocker.patch("ops.routers.users._notify_access_request")
    resp = mock_client.post(
        "/request-access",
        data={"display_name": "Test User", "requested_role": "viewer"},
        headers={"X-Auth-Request-Email": "user@example.com"},
    )
    assert resp.status_code == 200
    assert "submitted" in resp.text.lower()


def test_post_request_access_stores_email_when_opted_in(
    mock_client, mock_cursor_context, mocker
):
    """notify_email=on → notification_email stored in INSERT."""
    _, cursor = mock_cursor_context
    cursor.fetchone.side_effect = [None, None]
    mocker.patch("ops.routers.users._notify_access_request")
    mock_client.post(
        "/request-access",
        data={
            "display_name": "Test User",
            "requested_role": "viewer",
            "notify_email": "on",
        },
        headers={"X-Auth-Request-Email": "user@example.com"},
    )
    insert_call = cursor.execute.call_args_list[-1]
    params = insert_call[0][1]
    assert params[3] == "user@example.com"


def test_post_request_access_no_email_when_not_opted_in(
    mock_client, mock_cursor_context, mocker
):
    """No notify_email → notification_email is None in INSERT."""
    _, cursor = mock_cursor_context
    cursor.fetchone.side_effect = [None, None]
    mocker.patch("ops.routers.users._notify_access_request")
    mock_client.post(
        "/request-access",
        data={"display_name": "Test User", "requested_role": "viewer"},
        headers={"X-Auth-Request-Email": "user@example.com"},
    )
    insert_call = cursor.execute.call_args_list[-1]
    params = insert_call[0][1]
    assert params[3] is None


# ---------------------------------------------------------------------------
# ops/email.py
# ---------------------------------------------------------------------------

def test_send_access_approved_calls_resend(mocker):
    mock_send = mocker.patch("resend.Emails.send")
    from ops.email import send_access_approved
    send_access_approved("user@example.com", "viewer")
    mock_send.assert_called_once()
    call_args = mock_send.call_args[0][0]
    assert call_args["to"] == "user@example.com"
    assert "approved" in call_args["subject"].lower()
    assert "dashboard" in call_args["html"]


def test_send_access_denied_calls_resend(mocker):
    mock_send = mocker.patch("resend.Emails.send")
    from ops.email import send_access_denied
    send_access_denied("user@example.com")
    mock_send.assert_called_once()
    call_args = mock_send.call_args[0][0]
    assert call_args["to"] == "user@example.com"
    assert "not approved" in call_args["subject"].lower()


def test_send_access_approved_swallows_exception(mocker):
    mocker.patch("resend.Emails.send", side_effect=Exception("API down"))
    from ops.email import send_access_approved
    send_access_approved("user@example.com", "viewer")  # should not raise


def test_send_access_denied_swallows_exception(mocker):
    mocker.patch("resend.Emails.send", side_effect=Exception("API down"))
    from ops.email import send_access_denied
    send_access_denied("user@example.com")  # should not raise
