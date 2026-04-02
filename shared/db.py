"""
Shared psycopg2 connection helper and context manager.
"""
import logging
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

logger = logging.getLogger(__name__)

DB_KWARGS = {
    "host": os.environ.get("PGHOST", "postgres"),
    "port": int(os.environ.get("PGPORT", "5432")),
    "dbname": os.environ.get("PGDATABASE", "cartracker"),
    "user": os.environ.get("PGUSER", "cartracker"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}


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
        logger.error(msg)
        raise
    except Exception:
        msg = f"{error_context}: encountered DB error."
        logger.error(msg)
        raise

    try:
        cursor_factory = RealDictCursor if dict_cursor else None
        with conn.cursor(cursor_factory=cursor_factory) as cur:
            yield cur
            conn.commit()
    except Exception:
        msg = f"{error_context}: SQL execution failed."
        logger.error(msg)
        conn.rollback()
        raise
    finally:
        conn.close()
