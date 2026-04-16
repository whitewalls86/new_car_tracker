"""
Layer 3 — auth check and observer middleware integration tests.

Auth check tests seed an authorized_users row via seed_user_committed, then
send the raw email in X-Auth-Request-Email.  The route hashes it with the
test-salt and looks it up — verifying the full email→hash→DB→response cycle
that unit tests with mocked cursors cannot exercise.

Observer middleware tests don't seed any users; the role comes from the
X-User-Role header directly.
"""
import pytest

# ---------------------------------------------------------------------------
# Auth check — GET /auth/check
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_auth_check_authorized_user_returns_200(
    api_client, seed_user_committed, auth_email_hash, test_key_prefix
):
    email = f"{test_key_prefix}admin@test.local"
    seed_user_committed(auth_email_hash(email), "admin")

    response = api_client.get(
        "/auth/check", headers={"X-Auth-Request-Email": email}
    )

    assert response.status_code == 200
    assert response.headers["x-user-role"] == "admin"


@pytest.mark.integration
def test_auth_check_unknown_email_returns_403(api_client, test_key_prefix):
    response = api_client.get(
        "/auth/check",
        headers={"X-Auth-Request-Email": f"{test_key_prefix}nobody@test.local"},
    )
    assert response.status_code == 403


@pytest.mark.integration
def test_auth_check_no_email_header_returns_403(api_client):
    response = api_client.get("/auth/check")
    assert response.status_code == 403


@pytest.mark.integration
def test_auth_check_require_admin_passes_admin(
    api_client, seed_user_committed, auth_email_hash, test_key_prefix
):
    email = f"{test_key_prefix}admin2@test.local"
    seed_user_committed(auth_email_hash(email), "admin")

    response = api_client.get(
        "/auth/check?require=admin",
        headers={"X-Auth-Request-Email": email},
    )

    assert response.status_code == 200


@pytest.mark.integration
def test_auth_check_require_admin_fails_viewer(
    api_client, seed_user_committed, auth_email_hash, test_key_prefix
):
    email = f"{test_key_prefix}viewer@test.local"
    seed_user_committed(auth_email_hash(email), "viewer")

    response = api_client.get(
        "/auth/check?require=admin",
        headers={"X-Auth-Request-Email": email},
    )

    assert response.status_code == 403


@pytest.mark.integration
def test_auth_check_require_observer_passes_power_user(
    api_client, seed_user_committed, auth_email_hash, test_key_prefix
):
    email = f"{test_key_prefix}poweruser@test.local"
    seed_user_committed(auth_email_hash(email), "power_user")

    response = api_client.get(
        "/auth/check?require=observer",
        headers={"X-Auth-Request-Email": email},
    )

    # power_user tier > observer tier → should pass
    assert response.status_code == 200


@pytest.mark.integration
def test_auth_check_viewer_role_returned_correctly(
    api_client, seed_user_committed, auth_email_hash, test_key_prefix
):
    email = f"{test_key_prefix}viewer2@test.local"
    seed_user_committed(auth_email_hash(email), "viewer")

    response = api_client.get(
        "/auth/check", headers={"X-Auth-Request-Email": email}
    )

    assert response.status_code == 200
    assert response.headers["x-user-role"] == "viewer"


# ---------------------------------------------------------------------------
# Observer middleware
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_observer_cannot_mutate(api_client):
    response = api_client.post(
        "/admin/searches/",
        data={
            "search_key": "l3test-obs-block",
            "makes": "honda",
            "models": "civic",
            "zip": "10001",
        },
        headers={"x-user-role": "observer"},
        follow_redirects=False,
    )
    assert response.status_code == 403
    assert "Observers cannot make changes." in response.text


@pytest.mark.integration
def test_observer_can_read(api_client):
    response = api_client.get(
        "/admin/searches/", headers={"x-user-role": "observer"}
    )
    assert response.status_code == 200


@pytest.mark.integration
def test_observer_exempt_auth_check(api_client):
    # Observer POSTing to /auth/check is on the exempt path list — middleware
    # does not block it.  FastAPI returns 405 (no POST handler), not 403.
    response = api_client.post(
        "/auth/check",
        headers={"x-user-role": "observer"},
        follow_redirects=False,
    )
    assert response.status_code != 403
    assert "Observers cannot make changes." not in response.text


@pytest.mark.integration
def test_non_observer_can_mutate(api_client):
    # Middleware does not block admin role.  Route handler fires and returns
    # 422 for the invalid zip — not a 403 from the middleware.
    response = api_client.post(
        "/admin/searches/",
        data={
            "search_key": "l3test-admin-mutate",
            "makes": "honda",
            "models": "civic",
            "zip": "bad-zip",
        },
        headers={"x-user-role": "admin"},
        follow_redirects=False,
    )
    assert response.status_code == 422
