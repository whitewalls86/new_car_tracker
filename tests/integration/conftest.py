"""
Integration test fixtures.

These tests run against a real Postgres instance with Flyway migrations applied.
Set TEST_DATABASE_URL to override the connection string (defaults to CI values).
"""
import os
import uuid

import psycopg2
import pytest
from psycopg2.extras import RealDictCursor

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

_DEFAULT_URL = "postgresql://cartracker:cartracker@localhost:5432/cartracker"
DATABASE_URL = os.environ.get("TEST_DATABASE_URL", _DEFAULT_URL)

_DEFAULT_VIEWER_URL = "postgresql://viewer:ci_viewer@localhost:5432/cartracker"
VIEWER_DATABASE_URL = os.environ.get("TEST_VIEWER_DATABASE_URL", _DEFAULT_VIEWER_URL)


def _parse_dsn(url: str) -> dict:
    """Convert a postgresql:// URL into psycopg2 connect kwargs."""
    from urllib.parse import urlparse
    p = urlparse(url)
    return {
        "host": p.hostname or "localhost",
        "port": p.port or 5432,
        "dbname": p.path.lstrip("/") or "cartracker",
        "user": p.username or "cartracker",
        "password": p.password or "cartracker",
    }


@pytest.fixture(scope="session")
def db_conn_factory():
    """Returns a callable that creates new DB connections."""
    dsn = _parse_dsn(DATABASE_URL)

    def _connect():
        return psycopg2.connect(**dsn)

    # Smoke-test the connection once at session start
    conn = _connect()
    conn.close()
    return _connect


@pytest.fixture()
def db_conn(db_conn_factory):
    """
    Per-test connection that rolls back on teardown.

    Every test runs inside a transaction that is never committed,
    so tests cannot leave behind stale data.
    """
    conn = db_conn_factory()
    conn.autocommit = False
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture()
def cur(db_conn):
    """Convenience cursor (RealDictCursor) from the per-test connection."""
    with db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
        yield cursor


@pytest.fixture(scope="session")
def viewer_conn_factory():
    """Returns a callable that creates new DB connections as the viewer role."""
    dsn = _parse_dsn(VIEWER_DATABASE_URL)

    def _connect():
        return psycopg2.connect(**dsn)

    conn = _connect()
    conn.close()
    return _connect


@pytest.fixture()
def viewer_conn(viewer_conn_factory):
    """Per-test viewer connection that rolls back on teardown."""
    conn = viewer_conn_factory()
    conn.autocommit = False
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture()
def viewer_cur(viewer_conn):
    """Convenience cursor (RealDictCursor) connected as the viewer role."""
    with viewer_conn.cursor(cursor_factory=RealDictCursor) as cursor:
        yield cursor


# ---------------------------------------------------------------------------
# Seed helpers — minimal rows for parameterised queries
# ---------------------------------------------------------------------------

@pytest.fixture()
def seed_search_config(cur):
    """Insert a minimal search_configs row. Returns the search_key."""
    key = f"test-config-{uuid.uuid4().hex[:8]}"
    cur.execute(
        """
        INSERT INTO search_configs
            (search_key, enabled, params, rotation_order, created_at, updated_at)
        VALUES (%s, true, '{"makes": ["test"]}'::jsonb, 1, now(), now())
        """,
        (key,),
    )
    return key


@pytest.fixture()
def seed_authorized_user(cur):
    """Insert a minimal authorized_users row. Returns (id, email_hash)."""
    email_hash = f"testhash_{uuid.uuid4().hex[:12]}"
    cur.execute(
        """
        INSERT INTO authorized_users (email_hash, role, display_name)
        VALUES (%s, 'admin', 'Test Admin')
        RETURNING id
        """,
        (email_hash,),
    )
    user_id = cur.fetchone()["id"]
    return user_id, email_hash


@pytest.fixture()
def seed_access_request(cur):
    """Insert a minimal access_requests row. Returns (id, email_hash)."""
    email_hash = f"requesthash_{uuid.uuid4().hex[:12]}"
    cur.execute(
        """
        INSERT INTO access_requests (email_hash, requested_role, status)
        VALUES (%s, 'viewer', 'pending')
        RETURNING id
        """,
        (email_hash,),
    )
    req_id = cur.fetchone()["id"]
    return req_id, email_hash
