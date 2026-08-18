"""Saved SQL used to build the analytics serving snapshot."""

from pathlib import Path

from shared.query_loader import load_query

_SQL_DIR = Path(__file__).parent / "sql"

ANALYTICS_METRICS_SNAPSHOT = load_query(_SQL_DIR, "analytics_metrics_snapshot")
PUBLIC_STATS_SNAPSHOT = load_query(_SQL_DIR, "public_stats_snapshot")

ANALYTICS_METRIC_COLUMNS = (
    "cartracker_observation_count_last_hour",
    "cartracker_artifact_count_last_hour",
    "cartracker_block_events_last_hour",
    "cartracker_extraction_yield_last_day",
    "cartracker_stale_listings_pct",
    "cartracker_cooldown_backlog",
    "cartracker_cooldown_permanent",
    "data_through",
)

PUBLIC_STATS_COLUMNS = (
    "active_listings",
    "price_observations",
    "make_model_pairs",
    "artifacts_per_hour",
    "observations_per_hour",
)
