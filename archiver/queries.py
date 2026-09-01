"""
SQL query constants for the archiver service.

Queries are loaded from .sql files at import time so they can be:
  - run directly in psql for debugging
  - diffed cleanly in code review
  - loaded by integration tests to run the exact SQL used in production
"""
from pathlib import Path

from shared.query_loader import load_query

_SQL_DIR = Path(__file__).parent / "sql"


def _q(name: str) -> str:
    return load_query(_SQL_DIR, name)


# Plan 97: artifacts_queue cleanup
GET_QUEUE_CLEANUP_CANDIDATES = _q("get_queue_cleanup_candidates")
DELETE_CLEANUP_CANDIDATES = _q("delete_cleanup_candidates")
