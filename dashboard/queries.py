"""
SQL query loader for the dashboard service.

Loads all .sql files from dashboard/sql/ at import time and exposes them
as module-level constants. File name (without extension) becomes the constant
name in UPPER_CASE.

Usage:
    from dashboard.queries import STALE_VEHICLE_BACKLOG, COOLDOWN_BACKLOG
    from dashboard.queries import SUCCESS_RATE  # use .format(artifact_type=..., interval=...)
"""
from pathlib import Path

_SQL_DIR = Path(__file__).parent / "sql"


def _load(filename: str) -> str:
    return (_SQL_DIR / filename).read_text()


ACTIVE_RUNS = _load("active_runs.sql")
DBT_LOCK_STATUS = _load("dbt_lock_status.sql")
ROTATION_SCHEDULE = _load("rotation_schedule.sql")
RECENT_DETAIL_RUNS = _load("recent_detail_runs.sql")
STALE_VEHICLE_BACKLOG = _load("stale_vehicle_backlog.sql")
COOLDOWN_BACKLOG = _load("cooldown_backlog.sql")
PRICE_FRESHNESS = _load("price_freshness.sql")
BLOCKED_COOLDOWN_HISTOGRAM = _load("blocked_cooldown_histogram.sql")
# Template — call .format(artifact_type=..., interval=...) before executing
SUCCESS_RATE = _load("success_rate.sql")
SEARCH_SCRAPE_JOBS = _load("search_scrape_jobs.sql")
RUNS_OVER_TIME = _load("runs_over_time.sql")
ARTIFACT_BACKLOG = _load("artifact_backlog.sql")
TERMINATED_RUNS = _load("terminated_runs.sql")
PIPELINE_ERRORS = _load("pipeline_errors.sql")
DBT_BUILD_HISTORY = _load("dbt_build_history.sql")
PROCESSOR_ACTIVITY = _load("processor_activity.sql")
PROCESSING_THROUGHPUT = _load("processing_throughput.sql")
DETAIL_EXTRACTION_COVERAGE = _load("detail_extraction_coverage.sql")
PG_STAT_CONNECTIONS = _load("pg_stat_connections.sql")
PG_STAT_SLOW_QUERIES = _load("pg_stat_slow_queries.sql")
AIRFLOW_DAG_RUNS = _load("airflow_dag_runs.sql")
