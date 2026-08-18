"""Custom Prometheus gauges backed by the configured analytics reader."""
import logging
import math
import os
import time
from numbers import Number
from typing import Any

import requests as http_requests
from prometheus_client import Gauge

logger = logging.getLogger(__name__)

_ANALYTICS_READER_URL = os.environ.get(
    "ANALYTICS_READER_URL",
    os.environ.get("DBT_RUNNER_URL", "http://dbt_runner:8080"),
)
_REQUEST_TIMEOUT_SECONDS = 10

cartracker_observation_count_last_hour = Gauge(
    "cartracker_observation_count_last_hour",
    "Number of observations in the most recent complete scrape hour",
)

cartracker_artifact_count_last_hour = Gauge(
    "cartracker_artifact_count_last_hour",
    "Number of scrape artifacts processed in the most recent hour",
)

cartracker_block_events_last_hour = Gauge(
    "cartracker_block_events_last_hour",
    "Total 403 blocking events in the most recent hour",
)

cartracker_extraction_yield_last_day = Gauge(
    "cartracker_extraction_yield_last_day",
    "VIN extraction yield percentage from detail scrapes (most recent day)",
)

cartracker_stale_listings_pct = Gauge(
    "cartracker_stale_listings_pct",
    "Percentage of tracked vehicle VINs with price data older than 14 days",
)

cartracker_cooldown_backlog = Gauge(
    "cartracker_cooldown_backlog",
    "Listings in active cooldown (1-4 attempts - will retry)",
)

cartracker_cooldown_permanent = Gauge(
    "cartracker_cooldown_permanent",
    "Listings effectively blocked (5+ attempts - unlikely to clear)",
)

cartracker_metrics_last_success_timestamp_seconds = Gauge(
    "cartracker_metrics_last_success_timestamp_seconds",
    "Unix timestamp of the last fully successful dbt-backed metrics refresh",
)

_DATA_GAUGES = {
    "cartracker_observation_count_last_hour": cartracker_observation_count_last_hour,
    "cartracker_artifact_count_last_hour": cartracker_artifact_count_last_hour,
    "cartracker_block_events_last_hour": cartracker_block_events_last_hour,
    "cartracker_extraction_yield_last_day": cartracker_extraction_yield_last_day,
    "cartracker_stale_listings_pct": cartracker_stale_listings_pct,
    "cartracker_cooldown_backlog": cartracker_cooldown_backlog,
    "cartracker_cooldown_permanent": cartracker_cooldown_permanent,
}


def _set_all_data_gauges_nan() -> None:
    for gauge in _DATA_GAUGES.values():
        gauge.set(math.nan)


def _apply_values(values: Any) -> bool:
    """Publish valid values and fail only missing/invalid gauges to NaN."""
    if not isinstance(values, dict):
        _set_all_data_gauges_nan()
        return False

    complete = True
    for metric_name, gauge in _DATA_GAUGES.items():
        value = values.get(metric_name)
        if isinstance(value, Number) and not isinstance(value, (bool, complex)):
            gauge.set(float(value))
        else:
            gauge.set(math.nan)
            complete = False
    return complete


def update_analytics_metrics() -> None:
    """Refresh dbt-backed gauges through the configured analytics reader.

    dbt_runner owns the writable DuckDB file. A build in progress, a transport
    failure, or a failed query is represented as NaN instead of silently
    retaining the previous value. The freshness timestamp advances only after
    every expected gauge was updated successfully.
    """
    try:
        response = http_requests.get(
            f"{_ANALYTICS_READER_URL}/analytics/metrics",
            timeout=_REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
    except (http_requests.RequestException, ValueError) as exc:
        _set_all_data_gauges_nan()
        logger.warning("dbt-backed metrics refresh failed: %s", exc)
        return

    if not isinstance(payload, dict):
        _set_all_data_gauges_nan()
        logger.warning("dbt-backed metrics refresh returned a non-object response")
        return

    complete = _apply_values(payload.get("values"))
    if payload.get("ok") is True and complete:
        cartracker_metrics_last_success_timestamp_seconds.set(time.time())
        return

    logger.warning(
        "dbt-backed metrics refresh was partial: %s",
        payload.get("errors", "invalid response"),
    )


# A process that has not completed its first refresh has no trustworthy value.
# Prometheus exposes these as NaN immediately rather than manufacturing zeroes.
_set_all_data_gauges_nan()
