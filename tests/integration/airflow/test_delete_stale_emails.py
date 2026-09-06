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

from shared.query_loader import load_query
from tests.sql_loader import queries

SQL = queries(__file__)

_DEFAULT_URL = "postgresql://cartracker:cartracker@localhost:5432/cartracker"

# Read through ``shared.query_loader``, not with ``read_text``, and the
# difference is the whole of Plan 162 Stage X's recorder: ``SqlText`` carries
# the file it came from, a plain ``str`` carries nothing, and this statement
# was executing against a real Postgres with nothing able to say which file the
# text came from. The gate reported it as executed-but-unattributable on its
# first CI run.
_SQL = load_query(Path(__file__).parents[3] / "airflow" / "sql", "delete_stale_emails")


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
        SQL("insert_access_requests"),
    )
    rows = db.fetchall()
    ids = [r["id"] for r in rows]

    yield ids

    db.execute(SQL("delete_access_requests"), (ids,))


@pytest.mark.integration
def test_stale_email_is_nulled(db, seeded_rows):
    db.execute(_SQL)

    db.execute(
        SQL("select_notification_email_from_access_requests"),
    )
    assert db.fetchone()["notification_email"] is None


@pytest.mark.integration
def test_recent_email_is_preserved(db, seeded_rows):
    db.execute(_SQL)

    db.execute(
        SQL("select_notification_email_from_access_requests_2"),
    )
    assert db.fetchone()["notification_email"] == "recent@example.com"
