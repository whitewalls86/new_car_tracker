"""
SQL query constants for the archiver service.

Queries are loaded from .sql files at import time so they can be:
  - run directly in psql for debugging
  - diffed cleanly in code review
  - loaded by integration tests to run the exact SQL used in production
"""
from pathlib import Path

_SQL = Path(__file__).parent / "sql"


def _q(name: str) -> str:
    return (_SQL / f"{name}.sql").read_text()


GET_EXPIRED_PARQUET_MONTHS = _q("get_expired_parquet_months")
MARK_PARQUET_DELETED       = _q("mark_parquet_deleted")
