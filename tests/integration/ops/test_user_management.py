"""
Layer 3 — authorized_users mutation integration tests.

Covers POST /admin/users/{id}/role and POST /admin/users/{id}/revoke.

Each test seeds its own user via seed_user_committed (function-scoped),
which handles teardown automatically.
"""
import pytest

# ---------------------------------------------------------------------------
# Role change
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_change_user_role_updates_db(
    api_client, verify_cur, seed_user_committed, auth_email_hash, test_key_prefix
):
    email = f"{test_key_prefix}role-change@test.local"
    user_id, email_hash = seed_user_committed(auth_email_hash(email), "viewer")

    response = api_client.post(
        f"/admin/users/{user_id}/role",
        data={"role": "observer"},
        follow_redirects=False,
    )
    assert response.status_code == 303

    verify_cur.execute(
        "SELECT role FROM authorized_users WHERE id = %s", (user_id,)
    )
    assert verify_cur.fetchone()["role"] == "observer"


@pytest.mark.integration
def test_change_user_role_invalid_role_no_change(
    api_client, verify_cur, seed_user_committed, auth_email_hash, test_key_prefix
):
    email = f"{test_key_prefix}invalid-role@test.local"
    user_id, email_hash = seed_user_committed(auth_email_hash(email), "viewer")

    response = api_client.post(
        f"/admin/users/{user_id}/role",
        data={"role": "superadmin"},
        follow_redirects=False,
    )
    assert response.status_code == 303

    verify_cur.execute(
        "SELECT role FROM authorized_users WHERE id = %s", (user_id,)
    )
    assert verify_cur.fetchone()["role"] == "viewer"


# ---------------------------------------------------------------------------
# Revoke
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_revoke_user_removes_row(
    api_client, verify_cur, seed_user_committed, auth_email_hash, test_key_prefix
):
    email = f"{test_key_prefix}revoke-me@test.local"
    user_id, email_hash = seed_user_committed(auth_email_hash(email), "viewer")

    response = api_client.post(
        f"/admin/users/{user_id}/revoke", follow_redirects=False
    )
    assert response.status_code == 303

    verify_cur.execute(
        "SELECT 1 FROM authorized_users WHERE id = %s", (user_id,)
    )
    assert verify_cur.fetchone() is None


@pytest.mark.integration
def test_revoke_nonexistent_user_no_error(api_client):
    response = api_client.post(
        "/admin/users/99999/revoke", follow_redirects=False
    )
    assert response.status_code == 303
