"""
Integration test for the delete_stale_emails DAG SQL.

Validates the UPDATE logic directly against a real DB — no Airflow machinery
needed. Seeds a stale row (> 48h) and a recent row (< 48h), runs the exact
SQL the DAG uses, then asserts only the stale row was nulled.
"""
import os
from pathlib import Path

import psycopg2
import pytest
from psycopg2.extras import RealDictCursor

_DEFAULT_URL = "postgresql://cartracker:cartracker@localhost:5432/cartracker"

_SQL = (Path(__file__).parents[3] / "airflow" / "sql" / "delete_stale_emails.sql").read_text()


def _get_conn():
    from urllib.parse import urlparse
    url = os.environ.get("TEST_DATABASE_URL", _DEFAULT_URL)
    p = urlparse(url)
    return psycopg2.connect(
        host=p.hostname, port=p.port or 5432,
        dbname=p.path.lstrip("/"), user=p.username, password=p.password,
    )


@pytest.fixture()
def db():
    """Autocommit connection for seeding and verification."""
    conn = _get_conn()
    conn.autocommit = True
    yield conn.cursor(cursor_factory=RealDictCursor)
    conn.close()


@pytest.fixture()
def seeded_rows(db):
    """
    Seeds one stale row (requested_at 3 days ago) and one recent row
    (requested_at now). Both have notification_email set.
    Cleans up both rows after the test regardless of outcome.
    """
    db.execute(
        """
        INSERT INTO access_requests (email_hash, requested_role, notification_email, requested_at)
        VALUES
            ('test-stale-hash', 'viewer', 'stale@example.com', now() - interval '3 days'),
            ('test-recent-hash', 'viewer', 'recent@example.com', now())
        RETURNING id, email_hash
        """,
    )
    rows = db.fetchall()
    ids = [r["id"] for r in rows]

    yield ids

    db.execute("DELETE FROM access_requests WHERE id = ANY(%s)", (ids,))


@pytest.mark.integration
def test_stale_email_is_nulled(db, seeded_rows):
    db.execute(_SQL)

    db.execute(
        "SELECT notification_email FROM access_requests WHERE email_hash = 'test-stale-hash'",
    )
    assert db.fetchone()["notification_email"] is None


@pytest.mark.integration
def test_recent_email_is_preserved(db, seeded_rows):
    db.execute(_SQL)

    db.execute(
        "SELECT notification_email FROM access_requests WHERE email_hash = 'test-recent-hash'",
    )
    assert db.fetchone()["notification_email"] == "recent@example.com"
