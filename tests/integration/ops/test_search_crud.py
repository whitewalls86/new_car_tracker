"""
Layer 3 — search_configs CRUD integration tests.

Tests POST /admin/searches/ (create), GET /admin/searches/ (list),
POST /admin/searches/{key} (update), POST /admin/searches/{key}/toggle,
POST /admin/searches/{key}/delete against a real Postgres instance.

Teardown: deletes all rows whose search_key contains the module-scoped
test_key_prefix, including soft-deleted rows (renamed to _deleted_{key}_…).
"""
import os

import psycopg2
import pytest

_DEFAULT_URL = "postgresql://cartracker:cartracker@localhost:5432/cartracker"

_VALID_FORM = {
    "makes": "honda",
    "models": "cr-v",
    "zip": "10001",
    "scope_national": "true",
}


def _get_conn():
    from urllib.parse import urlparse
    url = os.environ.get("TEST_DATABASE_URL", _DEFAULT_URL)
    p = urlparse(url)
    return psycopg2.connect(
        host=p.hostname, port=p.port or 5432,
        dbname=p.path.lstrip("/"), user=p.username, password=p.password,
    )


@pytest.fixture(autouse=True, scope="module")
def cleanup_search_configs(test_key_prefix):
    yield
    conn = _get_conn()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM search_configs WHERE search_key LIKE %s OR search_key LIKE %s",
            (f"{test_key_prefix}%", f"_deleted_{test_key_prefix}%"),
        )
    conn.close()


# ---------------------------------------------------------------------------
# Create
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_create_search_persists_to_db(api_client, verify_cur, test_key_prefix):
    key = f"{test_key_prefix}honda-crv"
    response = api_client.post(
        "/admin/searches/",
        data={**_VALID_FORM, "search_key": key},
        follow_redirects=False,
    )
    assert response.status_code == 303

    verify_cur.execute(
        "SELECT params FROM search_configs WHERE search_key = %s", (key,)
    )
    row = verify_cur.fetchone()
    assert row is not None
    assert row["params"]["makes"] == ["honda"]
    assert row["params"]["models"] == ["cr-v"]
    assert row["params"]["zip"] == "10001"


@pytest.mark.integration
def test_create_search_params_shape_for_dbt(api_client, verify_cur, test_key_prefix):
    """All fields that stg_search_configs reads must be present in the params JSONB."""
    key = f"{test_key_prefix}params-shape"
    api_client.post(
        "/admin/searches/",
        data={**_VALID_FORM, "search_key": key},
        follow_redirects=False,
    )
    verify_cur.execute(
        "SELECT params FROM search_configs WHERE search_key = %s", (key,)
    )
    row = verify_cur.fetchone()
    assert row is not None
    for field in (
        "sort_order", "sort_rotation", "radius_miles", "max_listings", "max_safety_pages"):
        assert field in row["params"], f"params JSONB missing field: {field}"


@pytest.mark.integration
def test_create_search_duplicate_key_returns_422(api_client, test_key_prefix):
    key = f"{test_key_prefix}duplicate"
    data = {**_VALID_FORM, "search_key": key}
    api_client.post("/admin/searches/", data=data, follow_redirects=False)

    response = api_client.post("/admin/searches/", data=data, follow_redirects=False)

    assert response.status_code == 422
    assert "already exists" in response.text


@pytest.mark.integration
def test_create_search_invalid_zip_returns_422(api_client, verify_cur, test_key_prefix):
    key = f"{test_key_prefix}bad-zip"
    response = api_client.post(
        "/admin/searches/",
        data={**_VALID_FORM, "search_key": key, "zip": "not-a-zip"},
        follow_redirects=False,
    )

    assert response.status_code == 422

    verify_cur.execute(
        "SELECT 1 FROM search_configs WHERE search_key = %s", (key,)
    )
    assert verify_cur.fetchone() is None


# ---------------------------------------------------------------------------
# List
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_list_searches_includes_created_row(api_client, test_key_prefix):
    key = f"{test_key_prefix}list-check"
    api_client.post(
        "/admin/searches/",
        data={**_VALID_FORM, "search_key": key},
        follow_redirects=False,
    )

    response = api_client.get("/admin/searches/")

    assert response.status_code == 200
    assert key in response.text


# ---------------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_update_search_persists_to_db(api_client, verify_cur, test_key_prefix):
    key = f"{test_key_prefix}update-me"
    api_client.post(
        "/admin/searches/",
        data={**_VALID_FORM, "search_key": key, "radius_miles": "150"},
        follow_redirects=False,
    )

    api_client.post(
        f"/admin/searches/{key}",
        data={**_VALID_FORM, "radius_miles": "250"},
        follow_redirects=False,
    )

    verify_cur.execute(
        "SELECT params, updated_at, created_at FROM search_configs WHERE search_key = %s",
        (key,),
    )
    row = verify_cur.fetchone()
    assert row["params"]["radius_miles"] == 250
    assert row["updated_at"] >= row["created_at"]


# ---------------------------------------------------------------------------
# Toggle
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_toggle_search_flips_enabled(api_client, verify_cur, test_key_prefix):
    key = f"{test_key_prefix}toggle-me"
    # Create with enabled=False (default — checkbox not sent)
    api_client.post(
        "/admin/searches/",
        data={**_VALID_FORM, "search_key": key},
        follow_redirects=False,
    )
    verify_cur.execute(
        "SELECT enabled FROM search_configs WHERE search_key = %s", (key,)
    )
    assert verify_cur.fetchone()["enabled"] is False

    # First toggle → True
    api_client.post(f"/admin/searches/{key}/toggle", follow_redirects=False)
    verify_cur.execute(
        "SELECT enabled FROM search_configs WHERE search_key = %s", (key,)
    )
    assert verify_cur.fetchone()["enabled"] is True

    # Second toggle → back to False
    api_client.post(f"/admin/searches/{key}/toggle", follow_redirects=False)
    verify_cur.execute(
        "SELECT enabled FROM search_configs WHERE search_key = %s", (key,)
    )
    assert verify_cur.fetchone()["enabled"] is False


# ---------------------------------------------------------------------------
# Delete (soft)
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_delete_search_renames_key(api_client, verify_cur, test_key_prefix):
    key = f"{test_key_prefix}delete-me"
    api_client.post(
        "/admin/searches/",
        data={**_VALID_FORM, "search_key": key},
        follow_redirects=False,
    )

    response = api_client.post(
        f"/admin/searches/{key}/delete", follow_redirects=False
    )
    assert response.status_code == 303

    # Original key is gone
    verify_cur.execute(
        "SELECT 1 FROM search_configs WHERE search_key = %s", (key,)
    )
    assert verify_cur.fetchone() is None

    # Renamed row exists, disabled
    verify_cur.execute(
        "SELECT search_key, enabled FROM search_configs WHERE search_key LIKE %s",
        (f"_deleted_{key}%",),
    )
    row = verify_cur.fetchone()
    assert row is not None
    assert row["enabled"] is False
