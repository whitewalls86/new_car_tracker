"""Immutable presentation cache for the Plan 143 analytics snapshot."""

from __future__ import annotations

import json
import logging
import math
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from numbers import Number
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
DEFAULT_SNAPSHOT_PATH = "/data/analytics_snapshot/analytics_snapshot.json"
# Derived from the producer's cadence, not chosen. ``hourly_analytics_refresh``
# writes this snapshot on ``0 * * * *``, so successive successes are 3600s apart
# and anything shorter than that labels a healthy snapshot stale. It was 900,
# which meant the public page read "(stale)" for 45 of every 60 minutes -- a
# signal that is on three quarters of the time teaches a reader to ignore the one
# word that matters during a real outage. The 300s of slack absorbs queueing;
# past it, a run is genuinely late. ``tests/ops/test_public_stats.py`` fails if
# the DAG's schedule and this constant stop agreeing.
DAG_REFRESH_INTERVAL_SECONDS = 3600
STALE_GRACE_SECONDS = 300
DEFAULT_STALE_SECONDS = DAG_REFRESH_INTERVAL_SECONDS + STALE_GRACE_SECONDS

# ``data_through`` is a bucket *label*, not an end timestamp.
# ``mart_scrape_volume`` buckets on ``date_trunc('hour', fetched_at)``, and
# Plan 136 filters the snapshot query to ``hour < date_trunc('hour', now())``
# so only complete hours are published. So a snapshot naming 14:00 describes
# the 14:00-15:00 bucket, and the data it summarises is complete through 15:00.
#
# Rendering the label under the words "Analytics data through" understated the
# page's own freshness by a full hour -- at 15:48 it read 14:00, when the data
# really did run to 15:00. Nobody reads a dashboard freshness stamp as a bucket
# label, so the page adds the bucket width and shows the end.
#
# This is presentation only. The snapshot file and the Prometheus gauges keep
# the bucket label, which is right for them: it names the hour the counts
# describe, and the page never shows the counts' hour beside them.
# ``tests/ops/test_public_stats.py`` fails if the mart stops bucketing hourly.
MART_BUCKET_SECONDS = 3600
PUBLIC_STAT_NAMES = frozenset(
    {
        "active_listings",
        "price_observations",
        "make_model_pairs",
        "artifacts_per_hour",
        "observations_per_hour",
    }
)


@dataclass(frozen=True)
class PresentationSnapshot:
    stats: Mapping[str, int | str]
    status: str
    stale: bool
    last_success_at: str | None
    error: str | None = None


def _empty_snapshot(error: str | None = None) -> PresentationSnapshot:
    return PresentationSnapshot(
        stats=MappingProxyType({}),
        status="not_ready",
        stale=True,
        last_success_at=None,
        error=error,
    )


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("snapshot timestamp must be a string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("snapshot timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def _parse_snapshot(
    document: object,
    *,
    now: datetime,
    stale_seconds: int,
) -> PresentationSnapshot:
    if not isinstance(document, dict) or document.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported analytics snapshot schema")
    refresh = document.get("refresh")
    if not isinstance(refresh, dict):
        raise ValueError("analytics snapshot refresh metadata is missing")
    status = refresh.get("status")
    if status not in {"ok", "failed", "not_ready"}:
        raise ValueError("unsupported analytics snapshot status")

    last_success_at = refresh.get("last_success_at")
    last_success = _timestamp(last_success_at) if last_success_at is not None else None
    stale = status != "ok" or last_success is None
    if last_success is not None:
        stale = stale or (now - last_success).total_seconds() > stale_seconds

    raw_stats = document.get("public_stats")
    if not isinstance(raw_stats, dict) or not set(raw_stats).issubset(PUBLIC_STAT_NAMES):
        raise ValueError("analytics snapshot public_stats are invalid")
    stats: dict[str, int | str] = {}
    for name, value in raw_stats.items():
        if not isinstance(value, Number) or isinstance(value, (bool, complex)):
            raise ValueError(f"analytics snapshot field {name} is not numeric")
        numeric = float(value)
        if not math.isfinite(numeric):
            raise ValueError(f"analytics snapshot field {name} is not finite")
        stats[name] = int(round(numeric))

    data_through = document.get("data_through")
    if data_through is not None:
        # The bucket's end, not its label -- see MART_BUCKET_SECONDS above.
        bucket_end = _timestamp(data_through) + timedelta(seconds=MART_BUCKET_SECONDS)
        stats["analytics_data_through_iso"] = bucket_end.isoformat().replace("+00:00", "Z")

    return PresentationSnapshot(
        stats=MappingProxyType(stats),
        status=status,
        stale=stale,
        last_success_at=last_success_at if isinstance(last_success_at, str) else None,
    )


class PublicStatsCache:
    """Load snapshot files outside request handling and publish immutable views."""

    def __init__(
        self,
        path: str | Path | None = None,
        *,
        stale_seconds: int | None = None,
    ) -> None:
        self.path = Path(
            path or os.environ.get("ANALYTICS_SNAPSHOT_PATH", DEFAULT_SNAPSHOT_PATH)
        )
        self.stale_seconds = stale_seconds or int(
            os.environ.get("ANALYTICS_SNAPSHOT_STALE_SECONDS", DEFAULT_STALE_SECONDS)
        )
        self._lock = threading.Lock()
        self._current = _empty_snapshot()

    def get(self) -> PresentationSnapshot:
        with self._lock:
            return self._current

    def refresh(self, *, now: datetime | None = None) -> PresentationSnapshot:
        timestamp = now or datetime.now(timezone.utc)
        try:
            document = json.loads(self.path.read_text(encoding="utf-8"))
            current = _parse_snapshot(
                document,
                now=timestamp,
                stale_seconds=self.stale_seconds,
            )
        except Exception as exc:
            logger.warning("analytics presentation cache refresh failed: %s", type(exc).__name__)
            previous = self.get()
            current = PresentationSnapshot(
                stats=previous.stats,
                status="not_ready" if not previous.stats else "unavailable",
                stale=True,
                last_success_at=previous.last_success_at,
                error=type(exc).__name__,
            )
        with self._lock:
            self._current = current
        return current


public_stats_cache = PublicStatsCache()
