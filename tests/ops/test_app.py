

def test_get_health(mock_client):
    response = mock_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"ok": True}


def test_metrics_endpoint_returns_200(mock_client):
    response = mock_client.get("/metrics")
    assert response.status_code == 200


def test_metrics_endpoint_content_type_is_prometheus(mock_client):
    response = mock_client.get("/metrics")
    assert "text/plain" in response.headers["content-type"]


def test_metrics_endpoint_contains_http_requests_total(mock_client):
    # Make a request first so the counter exists in the registry.
    mock_client.get("/health")
    response = mock_client.get("/metrics")
    assert "http_requests_total" in response.text


def test_get_admin(mock_client):
    response = mock_client.get("/admin", follow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == "/admin/searches/"


def test_get_base_url(mock_client):
    response = mock_client.get("/", follow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == "/admin/searches/"


# ---------------------------------------------------------------------------
# Observer middleware
# ---------------------------------------------------------------------------

def test_observer_blocked_on_post(mock_client, mock_cursor_context):
    resp = mock_client.post(
        "/admin/users/1/role",
        data={"role": "viewer"},
        headers={"X-User-Role": "observer"},
        follow_redirects=False,
    )
    assert resp.status_code == 403
    assert "Observers cannot make changes" in resp.text


def test_observer_allowed_on_get(mock_client, mock_cursor_context):
    _, cursor = mock_cursor_context
    cursor.fetchall.return_value = []
    resp = mock_client.get(
        "/admin/users",
        headers={"X-User-Role": "observer"},
    )
    assert resp.status_code == 200


def test_admin_allowed_on_post(mock_client, mock_cursor_context):
    resp = mock_client.post(
        "/admin/users/1/revoke",
        headers={"X-User-Role": "admin"},
        follow_redirects=False,
    )
    assert resp.status_code == 303


def test_observer_exempt_health(mock_client):
    resp = mock_client.get(
        "/health",
        headers={"X-User-Role": "observer"},
    )
    assert resp.status_code == 200
