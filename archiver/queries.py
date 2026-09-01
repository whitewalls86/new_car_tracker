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

# staging.silver_observations -> MinIO flush (flush_silver_observations.py).
# SELECT_SILVER_OBSERVATIONS_UP_TO_ID is a template: `.format(columns=...)`
# with the processor's _DB_COLUMNS, which is also the list the returned tuples
# are zipped against.
SELECT_MAX_SILVER_OBSERVATION_ID = _q("select_max_silver_observation_id")
SELECT_SILVER_OBSERVATIONS_UP_TO_ID = _q("select_silver_observations_up_to_id")
DELETE_SILVER_OBSERVATIONS_UP_TO_ID = _q("delete_silver_observations_up_to_id")

# staging.*_events -> MinIO flush (flush_staging_events.py). All three are
# templates over `table`/`pk` (and `columns`), filled from _TABLE_CONFIGS: one
# flush serves seven staging tables, and a relation name cannot be bound as a
# parameter.
SELECT_STAGING_MAX_PK = _q("select_staging_max_pk")
SELECT_STAGING_ROWS_UP_TO_PK = _q("select_staging_rows_up_to_pk")
DELETE_STAGING_ROWS_UP_TO_PK = _q("delete_staging_rows_up_to_pk")
