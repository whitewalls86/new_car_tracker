"""
Layer 3 ops API integration test fixtures.

Tests use FastAPI TestClient against a real Postgres instance — no mocked DB.
shared/db.py and ops.routers.auth both read env vars at module import time, so
env vars must be set before any ops imports reach the module level.
"""
import hashlib
import os
import tempfile
import uuid

# ---------------------------------------------------------------------------
# Env vars — set before importing the ops app so that shared/db.py builds
# DB_KWARGS and auth._SALT with the correct test values on first import.
# ---------------------------------------------------------------------------
os.environ.setdefault("PGHOST", "localhost")
os.environ.setdefault("PGPORT", "5432")
os.environ.setdefault("PGDATABASE", "cartracker")
os.environ.setdefault("PGUSER", "cartracker")
os.environ.setdefault("POSTGRES_PASSWORD", "cartracker")
# AUTH_EMAIL_SALT must be set before the app imports so that auth._SALT is
# initialised with the same value the test fixtures use to compute hashes.
os.environ["AUTH_EMAIL_SALT"] = "test-salt"
# ops/app.py calls os.makedirs on LOG_PATH's parent at import time; point to
# a writable temp location so the import doesn't fail outside Docker.
os.environ.setdefault("LOG_PATH", os.path.join(tempfile.gettempdir(), "ops_test.log"))

import psycopg2  # noqa: E402
import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from psycopg2.extras import RealDictCursor  # noqa: E402

from ops.app import app  # noqa: E402

_DEFAULT_URL = "postgresql://cartracker:cartracker@localhost:5432/cartracker"
_DATABASE_URL = os.environ.get("TEST_DATABASE_URL", _DEFAULT_URL)


def _parse_dsn(url: str) -> dict:
    from urllib.parse import urlparse
    p = urlparse(url)
    return {
        "host": p.hostname or "localhost",
        "port": p.port or 5432,
        "dbname": p.path.lstrip("/") or "cartracker",
        "user": p.username or "cartracker",
        "password": p.password or "cartracker",
    }


# ---------------------------------------------------------------------------
# Core fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def api_client():
    """Session-scoped TestClient for the ops FastAPI app."""
    with TestClient(app, raise_server_exceptions=True) as client:
        yield client


@pytest.fixture()
def verify_cur():
    """
    Function-scoped autocommit cursor for reading committed DB state after
    TestClient requests.  Uses RealDictCursor so rows come back as dicts.
    """
    conn = psycopg2.connect(**_parse_dsn(_DATABASE_URL))
    conn.autocommit = True
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        yield cur
    conn.close()


@pytest.fixture(scope="module")
def test_key_prefix():
    """
    Returns a uuid-based prefix string unique to this test module invocation.
    All test rows should be keyed with this prefix so module-level teardown
    fixtures can DELETE WHERE key LIKE 'l3test-%' without cross-module collisions.
    """
    return f"l3test-{uuid.uuid4().hex[:8]}-"


@pytest.fixture()
def auth_email_hash():
    """
    Returns a callable that computes the expected email hash for a given address.

    The ops auth route hashes X-Auth-Request-Email with AUTH_EMAIL_SALT before
    looking it up in authorized_users.  Tests use this to seed rows with the
    hash the route will produce at request time.

        email_hash = auth_email_hash("user@test.local")
        seed_user_committed(email_hash, "admin")
        client.get("/auth/check", headers={"X-Auth-Request-Email": "user@test.local"})
    """
    def _hash(email: str) -> str:
        return hashlib.sha256((os.environ["AUTH_EMAIL_SALT"] + email.lower()).encode()).hexdigest()

    return _hash


@pytest.fixture()
def seed_user_committed():
    """
    Factory fixture that inserts authorized_users rows via a direct autocommit
    connection, making them immediately visible to the app's db_cursor() calls.

    Usage:
        def test_something(seed_user_committed, auth_email_hash):
            user_id, email_hash = seed_user_committed(
                auth_email_hash("admin@test.local"), "admin"
            )

    All rows inserted by this fixture are deleted in teardown.
    """
    conn = psycopg2.connect(**_parse_dsn(_DATABASE_URL))
    conn.autocommit = True
    inserted_hashes = []

    def _seed(email_hash: str, role: str, display_name: str = "Test User"):
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """INSERT INTO authorized_users (email_hash, role, display_name)
                   VALUES (%s, %s, %s)
                   RETURNING id""",
                (email_hash, role, display_name),
            )
            user_id = cur.fetchone()["id"]
        inserted_hashes.append(email_hash)
        return user_id, email_hash

    yield _seed

    with conn.cursor() as cur:
        for email_hash in inserted_hashes:
            cur.execute(
                "DELETE FROM authorized_users WHERE email_hash = %s",
                (email_hash,),
            )
    conn.close()
