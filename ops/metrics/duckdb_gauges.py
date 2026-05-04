"""Custom Prometheus gauges for DuckDB mart tables (data health signals)."""
import logging
import os

import duckdb
from prometheus_client import Gauge

logger = logging.getLogger(__name__)

_DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/data/analytics/analytics.duckdb")

# Define gauges for data health metrics
cartracker_observation_count_last_hour = Gauge(
    "cartracker_observation_count_last_hour",
    "Number of observations in the most recent complete scrape hour"
)

cartracker_artifact_count_last_hour = Gauge(
    "cartracker_artifact_count_last_hour",
    "Number of scrape artifacts processed in the most recent hour"
)

cartracker_block_events_last_hour = Gauge(
    "cartracker_block_events_last_hour",
    "Total 403 blocking events in the most recent hour"
)

cartracker_extraction_yield_last_day = Gauge(
    "cartracker_extraction_yield_last_day",
    "VIN extraction yield percentage from detail scrapes (most recent day)"
)

cartracker_stale_listings_pct = Gauge(
    "cartracker_stale_listings_pct",
    "Percentage of tracked vehicle VINs with price data older than 14 days"
)

cartracker_cooldown_backlog_high = Gauge(
    "cartracker_cooldown_backlog_high",
    "Number of listings stuck in 11+ cooldown attempts (severe IP block backlog)"
)


def update_duckdb_metrics():
    """Query DuckDB marts and update all custom gauges. Called by Instrumentator callback."""
    try:
        with duckdb.connect(_DUCKDB_PATH, read_only=True) as con:
            # Observation count and artifact count from last hour
            try:
                row = con.execute(
                    "SELECT COALESCE(observation_count, 0), COALESCE(artifact_count, 0) "
                    "FROM main.mart_scrape_volume ORDER BY hour DESC LIMIT 1"
                ).fetchone()
                if row:
                    cartracker_observation_count_last_hour.set(row[0])
                    cartracker_artifact_count_last_hour.set(row[1])
            except Exception as e:
                logger.warning(f"Failed to update scrape volume metrics: {e}")

            # Block events from last hour
            try:
                row = con.execute(
                    "SELECT COALESCE(total_block_events, 0) FROM main.mart_block_rate "
                    "ORDER BY hour DESC LIMIT 1"
                ).fetchone()
                if row:
                    cartracker_block_events_last_hour.set(row[0])
            except Exception as e:
                logger.warning(f"Failed to update block rate metrics: {e}")

            # VIN extraction yield from most recent day
            try:
                row = con.execute(
                    "SELECT COALESCE(extraction_yield, 0) FROM main.mart_detail_batch_outcomes "
                    "ORDER BY obs_date DESC LIMIT 1"
                ).fetchone()
                if row:
                    cartracker_extraction_yield_last_day.set(row[0])
            except Exception as e:
                logger.warning(f"Failed to update extraction yield metrics: {e}")

            # Stale listings percentage
            try:
                row = con.execute(
                    """
                    SELECT COALESCE(
                        ROUND(
                            100.0 * SUM(stale_gt_14d) / NULLIF(
                                SUM(stale_gt_14d + fresh_lt_1d + fresh_1_3d +
                                    fresh_4_7d + fresh_8_14d), 0
                            ), 2
                        ), 0
                    ) FROM main.mart_price_freshness_trend
                    """
                ).fetchone()
                if row:
                    cartracker_stale_listings_pct.set(row[0])
            except Exception as e:
                logger.warning(f"Failed to update staleness metrics: {e}")

            # Cooldown backlog (11+ attempts)
            try:
                row = con.execute(
                    "SELECT COALESCE(listing_count, 0) FROM main.mart_cooldown_cohorts "
                    "WHERE attempt_bucket = '11+'"
                ).fetchone()
                if row:
                    cartracker_cooldown_backlog_high.set(row[0])
            except Exception as e:
                logger.warning(f"Failed to update cooldown backlog metrics: {e}")

    except Exception as e:
        if "Conflicting lock" in str(e):
            logger.warning(f"DuckDB connection skipped (write lock held by dbt): {e}")
        else:
            logger.error(f"DuckDB connection failed: {e}")
