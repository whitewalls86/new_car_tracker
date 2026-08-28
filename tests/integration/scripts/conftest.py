"""
Integration fixtures for the recovery scripts under ``scripts/``.

Plan 145 Stage 5 slice 2 is the first mode in ``reconcile_april_detail.py``
that writes to Postgres, and the properties that matter -- one transaction per
batch, rollback on failure, a durable receipt that survives the staging flush,
and four hot tables that do not move -- cannot be shown against a fake cursor.
So these run against a real database with the Flyway migrations applied.
"""
import os
import tempfile

import psycopg2
import pytest
from psycopg2.extras import RealDictCursor

# shared/db.py builds DB_KWARGS at import time, so the env has to be right
# before anything imports it.
os.environ.setdefault("PGHOST", "localhost")
os.environ.setdefault("PGPORT", "5432")
os.environ.setdefault("PGDATABASE", "cartracker")
os.environ.setdefault("PGUSER", "cartracker")
os.environ.setdefault("POSTGRES_PASSWORD", "cartracker")
os.environ.setdefault(
    "LOG_PATH", os.path.join(tempfile.gettempdir(), "scripts_test.log"),
)

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


@pytest.fixture(scope="session", autouse=True)
def _patch_shared_db_kwargs():
    """Point ``shared.db`` at the test database regardless of import order."""
    import shared.db

    original = dict(shared.db.DB_KWARGS)
    shared.db.DB_KWARGS.update(_parse_dsn(_DATABASE_URL))
    yield
    shared.db.DB_KWARGS.update(original)


@pytest.fixture()
def pg_conn():
    """Autocommit connection, for seeding and for verifying committed writes."""
    conn = psycopg2.connect(**_parse_dsn(_DATABASE_URL))
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture()
def vc(pg_conn):
    """Autocommit RealDictCursor -- shorthand for verify cursor."""
    with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
        yield cur


@pytest.fixture()
def writer_conn():
    """A separate non-autocommit connection, the way the writer opens one."""
    conn = psycopg2.connect(**_parse_dsn(_DATABASE_URL))
    conn.autocommit = False
    yield conn
    conn.close()
