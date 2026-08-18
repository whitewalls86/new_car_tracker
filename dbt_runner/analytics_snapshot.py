"""Versioned post-build analytics serving snapshot publication."""

from __future__ import annotations

import copy
import json
import math
import os
import re
import tempfile
import threading
import time
from datetime import datetime, timezone
from numbers import Number
from pathlib import Path
from typing import Any, Callable, Mapping

from dbt_runner.queries import (
    ANALYTICS_METRIC_COLUMNS,
    ANALYTICS_METRICS_SNAPSHOT,
    PUBLIC_STATS_COLUMNS,
    PUBLIC_STATS_SNAPSHOT,
)
from shared.analytics_connection import get_analytics_connection

SCHEMA_VERSION = 1
DEFAULT_SNAPSHOT_PATH = "/data/analytics_snapshot/analytics_snapshot.json"
METRIC_NAMES = ANALYTICS_METRIC_COLUMNS[:-1]
PUBLIC_STAT_NAMES = PUBLIC_STATS_COLUMNS
_PUBLIC_INTEGER_FIELDS = frozenset(PUBLIC_STAT_NAMES)
_MAX_ERROR_LENGTH = 500
_ABSOLUTE_PATH = re.compile(r"(?:[A-Za-z]:[\\/]|/)(?:[^\s:]+[\\/])*[^\s:]+")
_CREDENTIAL_URL = re.compile(r"(://)[^/@\s]+@")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _iso_timestamp(value: Any, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if not isinstance(value, str):
        raise ValueError("timestamp must be an ISO-8601 string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _finite_number(value: Any, field: str) -> float:
    if not isinstance(value, Number) or isinstance(value, (bool, complex)):
        raise ValueError(f"{field} must be numeric")
    numeric = float(value)
    if not math.isfinite(numeric):
        raise ValueError(f"{field} must be finite")
    return numeric


def _safe_error(exc: BaseException) -> str:
    message = " ".join(str(exc).split())
    message = _CREDENTIAL_URL.sub(r"\1[credentials]@", message)
    message = _ABSOLUTE_PATH.sub("[path]", message)
    bounded = message[:_MAX_ERROR_LENGTH]
    return f"{type(exc).__name__}: {bounded}" if bounded else type(exc).__name__


def empty_snapshot(*, error: str = "snapshot_not_ready") -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "backend": "duckdb",
        "refresh": {
            "status": "not_ready",
            "attempted_at": None,
            "last_success_at": None,
            "duration_seconds": 0.0,
        },
        "data_through": None,
        "metrics": {name: None for name in METRIC_NAMES},
        "public_stats": {},
        "errors": {"snapshot": error},
    }


def validate_snapshot(document: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and normalize a version-1 snapshot document."""
    if document.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported analytics snapshot schema_version")
    if document.get("backend") != "duckdb":
        raise ValueError("unsupported analytics snapshot backend")

    refresh = document.get("refresh")
    if not isinstance(refresh, Mapping):
        raise ValueError("refresh must be an object")
    status = refresh.get("status")
    if status not in {"ok", "failed", "not_ready"}:
        raise ValueError("unsupported refresh status")
    attempted_at = _iso_timestamp(refresh.get("attempted_at"), nullable=True)
    last_success_at = _iso_timestamp(refresh.get("last_success_at"), nullable=True)
    duration = _finite_number(refresh.get("duration_seconds", 0), "refresh.duration_seconds")
    if duration < 0:
        raise ValueError("refresh.duration_seconds must not be negative")

    raw_metrics = document.get("metrics")
    if not isinstance(raw_metrics, Mapping) or set(raw_metrics) != set(METRIC_NAMES):
        raise ValueError("metrics must contain exactly the required metric names")
    metrics: dict[str, float | None] = {}
    for name in METRIC_NAMES:
        value = raw_metrics[name]
        if value is None and status != "ok":
            metrics[name] = None
        else:
            metrics[name] = _finite_number(value, f"metrics.{name}")

    raw_public_stats = document.get("public_stats")
    if not isinstance(raw_public_stats, Mapping):
        raise ValueError("public_stats must be an object")
    if not set(raw_public_stats).issubset(PUBLIC_STAT_NAMES):
        raise ValueError("public_stats contains unsupported fields")
    public_stats: dict[str, int | float] = {}
    for name, value in raw_public_stats.items():
        numeric = _finite_number(value, f"public_stats.{name}")
        public_stats[name] = int(round(numeric)) if name in _PUBLIC_INTEGER_FIELDS else numeric

    data_through = _iso_timestamp(document.get("data_through"), nullable=True)
    if status == "ok" and (last_success_at is None or data_through is None):
        raise ValueError("successful snapshots require last_success_at and data_through")

    raw_errors = document.get("errors")
    if not isinstance(raw_errors, Mapping):
        raise ValueError("errors must be an object")
    errors: dict[str, str] = {}
    for key, value in raw_errors.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ValueError("snapshot errors must be strings")
        errors[key[:80]] = value[:_MAX_ERROR_LENGTH]

    return {
        "schema_version": SCHEMA_VERSION,
        "backend": "duckdb",
        "refresh": {
            "status": status,
            "attempted_at": attempted_at,
            "last_success_at": last_success_at,
            "duration_seconds": round(duration, 6),
        },
        "data_through": data_through,
        "metrics": metrics,
        "public_stats": public_stats,
        "errors": errors,
    }


def load_snapshot(path: str | Path) -> dict[str, Any]:
    snapshot_path = Path(path)
    try:
        document = json.loads(snapshot_path.read_text(encoding="utf-8"))
        if not isinstance(document, Mapping):
            raise ValueError("snapshot root must be an object")
        return validate_snapshot(document)
    except FileNotFoundError:
        return empty_snapshot()
    except (OSError, json.JSONDecodeError, ValueError, TypeError):
        return empty_snapshot(error="snapshot_invalid_or_unsupported")


def atomic_write_snapshot(path: str | Path, document: Mapping[str, Any]) -> None:
    """Durably replace the published JSON without exposing a partial file."""
    normalized = validate_snapshot(document)
    snapshot_path = Path(path)
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=snapshot_path.parent,
            prefix=f".{snapshot_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temp_path = Path(handle.name)
            json.dump(normalized, handle, allow_nan=False, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        for attempt in range(5):
            try:
                os.replace(temp_path, snapshot_path)
                break
            except PermissionError:
                if attempt == 4:
                    raise
                time.sleep(0.01)
        temp_path = None
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


def _result_row(connection: Any, sql: str, expected_columns: tuple[str, ...]) -> tuple[Any, ...]:
    result = connection.execute(sql)
    row = result.fetchone()
    if row is None:
        raise ValueError("snapshot query returned no rows")
    columns = tuple(column[0] for column in result.description)
    if columns != expected_columns:
        raise ValueError("snapshot query returned unexpected columns")
    return tuple(row)


class AnalyticsSnapshotManager:
    """Thread-safe in-memory state plus atomic persisted publication."""

    def __init__(self, path: str | Path | None = None) -> None:
        self.path = Path(
            path or os.environ.get("ANALYTICS_SNAPSHOT_PATH", DEFAULT_SNAPSHOT_PATH)
        )
        self._lock = threading.Lock()
        self._snapshot = load_snapshot(self.path)

    def get_snapshot(self) -> dict[str, Any]:
        with self._lock:
            return copy.deepcopy(self._snapshot)

    def _set_snapshot(self, snapshot: Mapping[str, Any]) -> None:
        normalized = validate_snapshot(snapshot)
        with self._lock:
            self._snapshot = normalized

    def refresh(
        self,
        *,
        connection_factory: Callable[[], Any] = get_analytics_connection,
        attempted_at: str | None = None,
    ) -> dict[str, Any]:
        attempted = _iso_timestamp(attempted_at or utc_now_iso())
        previous = self.get_snapshot()
        started = time.monotonic()
        try:
            with connection_factory() as connection:
                metric_row = _result_row(
                    connection, ANALYTICS_METRICS_SNAPSHOT, ANALYTICS_METRIC_COLUMNS
                )
                public_row = _result_row(
                    connection, PUBLIC_STATS_SNAPSHOT, PUBLIC_STATS_COLUMNS
                )

            metrics = {
                name: _finite_number(metric_row[index], f"metrics.{name}")
                for index, name in enumerate(METRIC_NAMES)
            }
            data_through = _iso_timestamp(metric_row[-1])
            public_stats = {
                name: int(round(_finite_number(public_row[index], f"public_stats.{name}")))
                for index, name in enumerate(PUBLIC_STAT_NAMES)
            }
            snapshot = {
                "schema_version": SCHEMA_VERSION,
                "backend": "duckdb",
                "refresh": {
                    "status": "ok",
                    "attempted_at": attempted,
                    "last_success_at": attempted,
                    "duration_seconds": time.monotonic() - started,
                },
                "data_through": data_through,
                "metrics": metrics,
                "public_stats": public_stats,
                "errors": {},
            }
            atomic_write_snapshot(self.path, snapshot)
            self._set_snapshot(snapshot)
            current = self.get_snapshot()
            return {
                "ok": True,
                "status": "ok",
                "attempted_at": attempted,
                "last_success_at": current["refresh"]["last_success_at"],
                "duration_seconds": current["refresh"]["duration_seconds"],
            }
        except Exception as exc:
            duration = time.monotonic() - started
            failure = copy.deepcopy(previous)
            failure["refresh"] = {
                "status": "failed",
                "attempted_at": attempted,
                "last_success_at": previous["refresh"]["last_success_at"],
                "duration_seconds": duration,
            }
            failure["errors"] = {"refresh": _safe_error(exc)}
            try:
                atomic_write_snapshot(self.path, failure)
            except Exception as persist_exc:
                failure["errors"]["persistence"] = _safe_error(persist_exc)
            self._set_snapshot(failure)
            return {
                "ok": False,
                "status": "failed",
                "attempted_at": attempted,
                "last_success_at": failure["refresh"]["last_success_at"],
                "duration_seconds": round(duration, 6),
                "error": failure["errors"]["refresh"],
            }
