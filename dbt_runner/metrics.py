"""Prometheus export for the in-memory analytics serving snapshot."""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Mapping

from prometheus_client import CollectorRegistry, Gauge

from dbt_runner.analytics_snapshot import METRIC_NAMES

_DESCRIPTIONS = {
    "cartracker_observation_count_last_hour": (
        "Number of observations in the most recent complete scrape hour"
    ),
    "cartracker_artifact_count_last_hour": (
        "Number of scrape artifacts processed in the most recent complete hour"
    ),
    "cartracker_block_events_last_hour": "Total 403 blocking events in the most recent hour",
    "cartracker_extraction_yield_last_day": (
        "VIN extraction yield percentage from detail scrapes on the most recent day"
    ),
    "cartracker_stale_listings_pct": (
        "Percentage of tracked vehicle VINs with price data older than 14 days"
    ),
    "cartracker_cooldown_backlog": "Listings in active cooldown with one to four attempts",
    "cartracker_cooldown_permanent": "Listings blocked after five or more attempts",
    "cartracker_cooldown_entries_7d_attempt_1": (
        "Cooldown transitions into attempt bucket 1 over the rolling seven-day window"
    ),
    "cartracker_cooldown_entries_7d_attempt_2": (
        "Cooldown transitions into attempt bucket 2 over the rolling seven-day window"
    ),
    "cartracker_cooldown_entries_7d_attempt_3_4": (
        "Cooldown transitions into attempt bucket 3-4 over the rolling seven-day window"
    ),
    "cartracker_cooldown_entries_7d_attempt_5_10": (
        "Cooldown transitions into attempt bucket 5-10 over the rolling seven-day window"
    ),
    "cartracker_cooldown_entries_7d_attempt_11_plus": (
        "Cooldown transitions into attempt bucket 11+ over the rolling seven-day window"
    ),
}

REGISTRY = CollectorRegistry()
DATA_GAUGES = {
    name: Gauge(name, _DESCRIPTIONS[name], registry=REGISTRY) for name in METRIC_NAMES
}
LAST_SUCCESS = Gauge(
    "cartracker_metrics_last_success_timestamp_seconds",
    "Unix timestamp of the last fully successful analytics snapshot publication",
    registry=REGISTRY,
)
REFRESH_SUCCESS = Gauge(
    "cartracker_analytics_snapshot_refresh_success",
    "Whether the latest analytics snapshot refresh completed successfully",
    registry=REGISTRY,
)
REFRESH_DURATION = Gauge(
    "cartracker_analytics_snapshot_refresh_duration_seconds",
    "Duration of the latest analytics snapshot refresh attempt",
    registry=REGISTRY,
)


def _timestamp_seconds(value: Any) -> float:
    if not isinstance(value, str):
        return math.nan
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def publish_snapshot(snapshot: Mapping[str, Any]) -> None:
    """Update gauges from memory; this function never opens analytics storage."""
    refresh = snapshot.get("refresh", {})
    status = refresh.get("status")
    metrics = snapshot.get("metrics", {})
    if status == "ok":
        for name, gauge in DATA_GAUGES.items():
            value = metrics.get(name)
            gauge.set(float(value) if value is not None else math.nan)
    else:
        for gauge in DATA_GAUGES.values():
            gauge.set(math.nan)

    LAST_SUCCESS.set(_timestamp_seconds(refresh.get("last_success_at")))
    REFRESH_SUCCESS.set(1 if status == "ok" else 0)
    REFRESH_DURATION.set(float(refresh.get("duration_seconds", 0)))


publish_snapshot(
    {
        "refresh": {
            "status": "not_ready",
            "last_success_at": None,
            "duration_seconds": 0,
        },
        "metrics": {},
    }
)
