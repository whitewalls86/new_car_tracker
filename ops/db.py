"""
Shared psycopg2 connection helper for the ops container.
"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

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
def db_cursor(dict_cursor=True):
    """Context manager that yields a cursor and commits on success."""
    conn = get_conn()
    try:
        cursor_factory = RealDictCursor if dict_cursor else None
        with conn.cursor(cursor_factory=cursor_factory) as cur:
            yield cur
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
