

from types import MappingProxyType

from ops.public_stats import PresentationSnapshot


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


def test_metrics_endpoint_does_not_proxy_analytics_metrics(mock_client):
    response = mock_client.get("/metrics")
    assert "cartracker_observation_count_last_hour" not in response.text
    assert "cartracker_metrics_last_success_timestamp_seconds" not in response.text


def test_app_has_no_analytics_gauge_loop():
    from ops import app

    assert not hasattr(app, "_analytics_metrics_loop")


def test_get_admin(mock_client):
    response = mock_client.get("/admin", follow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == "/admin/searches/"


def test_get_base_url_is_no_longer_the_admin_redirect(mock_client, mocker):
    """Plan 138 Stage 2 gave ``/`` to the public landing page.

    Caddy sent ``/`` to Streamlit before this stage, so this redirect was
    reachable only from inside the network. The contract now lives in
    tests/ops/routers/test_public_routes.py; this asserts the handover.
    """
    mocker.patch(
        "ops.routers.info.public_stats_cache.get",
        return_value=PresentationSnapshot(
            stats=MappingProxyType({}),
            status="not_ready",
            stale=True,
            last_success_at=None,
        ),
    )

    response = mock_client.get("/", follow_redirects=False)

    assert response.status_code == 200


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
