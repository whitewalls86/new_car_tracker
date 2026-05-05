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
