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

# ---------------------------------------------------------------------------
# Plan 120 lake snapshot pipeline — DuckDB over Parquet, not Postgres.
# ---------------------------------------------------------------------------
# These are the audit, selector-wrapping, cohort-closure and materialization
# statements. They sit in their own subdirectory because there are eleven of
# them and they belong to one pipeline, not to the archiver's Postgres flush
# paths above.
#
# Every one is a template, and the interpolated part is never a value. Three
# kinds occur: a Parquet path inside `read_parquet('...')`, which is a relation
# and not a column reference; a projection or column name; and a WHERE fragment
# whose *clause structure* varies by table -- blocked_cooldown_events has no
# vin, and only silver_observations admits exact artifact row keys outside the
# time window. Every value inside those fragments is still bound: the builders
# in the processors return `(sql, params)` and the params are passed through.
#
# `candidate_sql` in the two wrappers is the loudest case: it is another
# statement, itself loaded from archiver/sql/lake_snapshot_selectors/.
_LAKE_SNAPSHOT_SQL_DIR = _SQL_DIR / "lake_snapshot"


def _lake_q(name: str) -> str:
    return load_query(_LAKE_SNAPSHOT_SQL_DIR, name)


# lake_source_audit.py
SELECT_SOURCE_TABLE_STATS = _lake_q("select_source_table_stats")

# lake_snapshot_selectors.py
WRAP_AGGREGATE_QUERY = _lake_q("wrap_aggregate_query")

# lake_snapshot_cohort.py
WRAP_CANDIDATE_QUERY = _lake_q("wrap_candidate_query")
SELECT_ROW_KEYS_FOR_CANDIDATES = _lake_q("select_row_keys_for_candidates")
SELECT_SEED_VINS_BY_HASH = _lake_q("select_seed_vins_by_hash")
SELECT_VINS_RANKED_WITHIN_MAKE_MODEL = _lake_q("select_vins_ranked_within_make_model")
SELECT_LISTING_IDS_FOR_VINS = _lake_q("select_listing_ids_for_vins")
SELECT_VINS_FOR_LISTING_IDS = _lake_q("select_vins_for_listing_ids")
SELECT_PREVIOUS_LISTING_IDS = _lake_q("select_previous_listing_ids")
SELECT_ARTIFACT_IDS = _lake_q("select_artifact_ids")

# lake_snapshot_export.py
SELECT_FILTERED_TABLE_ROWS = _lake_q("select_filtered_table_rows")
