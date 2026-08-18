"""Immutable presentation cache for the Plan 143 analytics snapshot."""

from __future__ import annotations

import json
import logging
import math
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from numbers import Number
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
DEFAULT_SNAPSHOT_PATH = "/data/analytics_snapshot/analytics_snapshot.json"
DEFAULT_STALE_SECONDS = 900
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
        stats["analytics_data_through_iso"] = _timestamp(data_through).isoformat().replace(
            "+00:00", "Z"
        )

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
