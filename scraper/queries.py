"""
SQL query loader for the scraper service.

Loads .sql files from scraper/sql/ at import time and exposes them as
module-level constants. Mirrors the pattern used in processing/queries.py.
"""
from pathlib import Path

_SQL_DIR = Path(__file__).parent / "sql"


def _load(filename: str) -> str:
    return (_SQL_DIR / filename).read_text()


UPSERT_BLOCKED_COOLDOWN = _load("upsert_blocked_cooldown.sql")
GET_BLOCKED_COOLDOWN_ATTEMPTS = _load("get_blocked_cooldown_attempts.sql")
INSERT_BLOCKED_COOLDOWN_EVENT = _load("insert_blocked_cooldown_event.sql")
