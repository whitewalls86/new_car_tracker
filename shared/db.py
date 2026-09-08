"""
Shared psycopg2 connection helper and context manager.

Connection resolution order:
  1. DATABASE_URL  — full DSN string (used by the scraper service)
  2. PG* env vars  — PGHOST / PGPORT / PGDATABASE / PGUSER / POSTGRES_PASSWORD
                     (used by the archiver and processing services)
"""
import logging
import os
from contextlib import contextmanager
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

# The two failures that mean *the database could not be reached*, as opposed to
# the database reaching a verdict and refusing the statement. Callers that turn
# an exception into an HTTP status need the difference: only these justify
# "Database unavailable", and a caller that returns it for everything blames the
# one component that was healthy. Plan 162 Stage K.
UNREACHABLE_ERRORS = (psycopg2.OperationalError, psycopg2.InterfaceError)

_DATABASE_URL = os.environ.get("DATABASE_URL", "")

if _DATABASE_URL:
    _p = urlparse(_DATABASE_URL)
    DB_KWARGS = {
        "host":     _p.hostname or "postgres",
        "port":     _p.port or 5432,
        "dbname":   _p.path.lstrip("/") or "cartracker",
        "user":     _p.username or "cartracker",
        "password": _p.password or "",
    }
else:
    try:
        _pgport = int(os.environ.get("PGPORT", "5432"))
    except ValueError:
        raise ValueError(f"PGPORT must be an integer, got: {os.environ.get('PGPORT')!r}")
    DB_KWARGS = {
        "host":     os.environ.get("PGHOST", "postgres"),
        "port":     _pgport,
        "dbname":   os.environ.get("PGDATABASE", "cartracker"),
        "user":     os.environ.get("PGUSER", "cartracker"),
        "password": os.environ.get("POSTGRES_PASSWORD", ""),
    }


def db_failure_cause(exc: BaseException) -> str:
    """The one line of a database failure worth putting in a response.

    psycopg2 leads with the primary message -- the constraint that rejected the
    row, the statement that would not parse -- and appends DETAIL and CONTEXT
    blocks naming the failing row. The log keeps all of it through ``exc_info``;
    a response gets the first line, which is the part that names the cause.
    """
    lines = str(exc).strip().splitlines()
    return lines[0] if lines else exc.__class__.__name__


def get_conn():
    """Return a new psycopg2 connection."""
    return psycopg2.connect(**DB_KWARGS)


@contextmanager
def db_cursor(error_context="DB Operation", dict_cursor=False):
    """
    Context manager that yields a cursor, handles commit/rollback, and logs errors.

    Usage:
        with db_cursor(error_context="Get user") as cur:
            cur.execute("SELECT * FROM users WHERE id = %s", (123,))
            user = cur.fetchone()
    """
    conn = None
    try:
        conn = get_conn()
    except psycopg2.OperationalError:
        msg = f"{error_context}: Unable to connect to Postgres database."
        logger.error(msg, exc_info=True)
        raise
    except Exception:
        msg = f"{error_context}: encountered DB error."
        logger.error(msg, exc_info=True)
        raise

    try:
        cursor_factory = RealDictCursor if dict_cursor else None
        with conn.cursor(cursor_factory=cursor_factory) as cur:
            yield cur
            conn.commit()
    except Exception:
        msg = f"{error_context}: SQL execution failed."
        logger.error(msg, exc_info=True)
        conn.rollback()
        raise
    finally:
        conn.close()
