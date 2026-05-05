"""
SQL query loader for the scraper service.

Loads .sql files from scraper/sql/ at import time and exposes them as
module-level constants. Mirrors the pattern used in processing/queries.py.
"""
from pathlib import Path

from shared.query_loader import load_query

_SQL_DIR = Path(__file__).parent / "sql"


def _load(name: str) -> str:
    return load_query(_SQL_DIR, name)


UPSERT_BLOCKED_COOLDOWN = _load("upsert_blocked_cooldown")
GET_BLOCKED_COOLDOWN_ATTEMPTS = _load("get_blocked_cooldown_attempts")
INSERT_BLOCKED_COOLDOWN_EVENT = _load("insert_blocked_cooldown_event")
