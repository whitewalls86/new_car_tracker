"""
Persisted planning cache for CI lake snapshot selector/cohort planning
(Plan 120 Gate C.75).

Selector candidate collection and cohort allocation/closure are the two most
expensive phases of the heavy dry-run planning path
(dry_run=True + run_selectors=True + build_cohort=True). This module lets
equivalent planning requests reuse a previously computed result instead of
rescanning the lake.

The cache key (fingerprint) represents *semantic planning identity* only —
tier, selector/cohort toggles, the normalized source window, target_vins,
min_selector_coverage, source table paths, and hashes of the selector
config/SQL and cohort algorithm version. Execution/reporting-mode fields
(dry_run, audit_sources, snapshot_id) never affect the fingerprint, since they
don't change what planning would compute.

Callers MUST resolve the actual planning window via `resolve_planning_window`
*before* both running selectors/cohort and computing the fingerprint. The
fingerprint hashes whatever window it is given — if the caller fingerprints a
bucketed identity but then queries with a different (unbucketed) window, the
cache would key repeated-but-different computations under the same entry.
`resolve_planning_window` is the single source of truth for that window, so
the fingerprint and the actual query always agree.
"""
import hashlib
import json
import logging
import time
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from archiver.processors import lake_snapshot_selectors as _selectors_mod
from archiver.processors.lake_snapshot_cohort import CandidateSet
from archiver.processors.lake_source_audit import SOURCE_TABLE_SPECS, resolve_table_path
from shared.minio import read_json, write_json

logger = logging.getLogger("archiver")

CACHE_SCHEMA_VERSION = 1
COHORT_ALGORITHM_VERSION = 1

VALID_BUCKET_GRAINS: Tuple[str, ...] = ("week", "day", "none")
DEFAULT_PLANNING_CACHE_PREFIX = "snapshot_planning_cache"


def _iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat()


def _as_utc(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc) if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def _bucket_week_start(dt: datetime) -> datetime:
    """Stable Monday-00:00-UTC bucket for *dt*."""
    dt = _as_utc(dt)
    monday = dt - timedelta(days=dt.weekday())
    return monday.replace(hour=0, minute=0, second=0, microsecond=0)


def _bucket_day_start(dt: datetime) -> datetime:
    """Stable midnight-UTC bucket for *dt*."""
    dt = _as_utc(dt)
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def _days_in_month(year: int, month: int) -> int:
    import calendar
    return calendar.monthrange(year, month)[1]


def subtract_months(dt: datetime, months: int) -> datetime:
    total_months = dt.month - 1 - months
    year = dt.year + total_months // 12
    month = total_months % 12 + 1
    day = min(dt.day, _days_in_month(year, month))
    return dt.replace(year=year, month=month, day=day)


def resolve_planning_window(
    request, window_start: Optional[datetime], window_end: Optional[datetime],
    now: Optional[datetime] = None,
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """Resolve the window heavy planning should actually query.

    An explicit source_window_start/end, or bucket grain "none", pass
    *window_start*/*window_end* through unchanged. A relative
    source_window_months window with grain "week"/"day" is re-anchored to the
    bucketed "now" so the executed query matches the fingerprint's identity —
    two calls in the same bucket must compute (and therefore may safely
    share/reuse) the exact same window, not merely a same-shaped one.
    """
    if request.source_window_start is not None and request.source_window_end is not None:
        return window_start, window_end
    if request.source_window_months is None:
        return window_start, window_end

    grain = request.planning_cache_bucket_grain
    if grain == "none":
        return window_start, window_end

    now = now or datetime.now(timezone.utc)
    if grain == "week":
        bucketed_now = _bucket_week_start(now)
    elif grain == "day":
        bucketed_now = _bucket_day_start(now)
    else:
        raise ValueError(f"Unknown planning_cache_bucket_grain: {grain!r}")
    return subtract_months(bucketed_now, request.source_window_months), bucketed_now


def _hash_json(payload: Dict[str, Any]) -> str:
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def selector_config_hash() -> str:
    """Hash of the loaded selector config values (independent of SQL text)."""
    configs = _selectors_mod._SELECTOR_CONFIGS
    payload = {name: asdict(config) for name, config in configs.items()}
    return _hash_json(payload)


def selector_sql_hash() -> str:
    """Hash of the resolved SQL template text for every registered selector."""
    configs = _selectors_mod._SELECTOR_CONFIGS
    payload = {
        name: _selectors_mod._q(config.sql_template) for name, config in configs.items()
    }
    return _hash_json(payload)


def source_table_paths_hash(base_path: Optional[str]) -> str:
    """Hash of the resolved Parquet path for every supported source table.

    Distinct from source_base_path: this changes if the underlying table
    layout (SOURCE_TABLE_SPECS relative paths) or MinIO bucket changes even
    when base_path itself is unchanged (e.g. None -> s3://{BUCKET}/...), so a
    lake layout migration invalidates old cache entries.
    """
    payload = {
        table_name: resolve_table_path(table_name, base_path)
        for table_name in SOURCE_TABLE_SPECS
    }
    return _hash_json(payload)


def compute_fingerprint_window(
    window_start: Optional[datetime], window_end: Optional[datetime],
) -> Dict[str, Any]:
    """Normalize an already-resolved planning window into its identity shape.

    *window_start*/*window_end* must be the effective planning window (see
    `resolve_planning_window`) — this function does no bucketing of its own,
    it only hashes what it's given.
    """
    if window_start is None and window_end is None:
        return {"mode": "none"}
    return {
        "mode": "window",
        "start": _iso(window_start),
        "end": _iso(window_end),
    }


def compute_planning_fingerprint(
    request, window_start: Optional[datetime], window_end: Optional[datetime],
) -> Tuple[str, Dict[str, Any]]:
    """Compute the planning fingerprint and the payload it was hashed from.

    *window_start*/*window_end* must already be the effective planning window
    resolved by `resolve_planning_window` — not the raw/unbucketed window.
    """
    fingerprint_window = compute_fingerprint_window(window_start, window_end)
    payload = {
        "cache_schema_version": CACHE_SCHEMA_VERSION,
        "tier": request.tier,
        "run_selectors": request.run_selectors,
        "build_cohort": request.build_cohort,
        "fingerprint_window": fingerprint_window,
        "target_vins": request.target_vins,
        "min_selector_coverage": request.min_selector_coverage,
        "source_base_path": request.source_base_path,
        "source_table_paths_hash": source_table_paths_hash(request.source_base_path),
        "selector_config_hash": selector_config_hash(),
        "selector_sql_hash": selector_sql_hash(),
        "cohort_algorithm_version": COHORT_ALGORITHM_VERSION,
    }
    return _hash_json(payload), payload


def planning_cache_path(prefix: str, fingerprint: str) -> str:
    return f"{prefix.rstrip('/')}/fingerprints/{fingerprint}/planning.json"


def serialize_candidate_sets(candidate_sets: Dict[str, CandidateSet]) -> Dict[str, Any]:
    """Convert CandidateSet objects into a JSON-serializable shape."""
    return {
        name: {
            "selector_name": cs.selector_name,
            "entity_key": cs.entity_key,
            "required": cs.required,
            "entities": list(cs.entities),
            "candidate_rows": cs.candidate_rows,
            "selected_entities": list(cs.selected_entities),
            "status": cs.status,
            "error": cs.error,
            "entity_count": cs.entity_count,
        }
        for name, cs in candidate_sets.items()
    }


def build_planning_cache_artifact(
    *,
    fingerprint: str,
    request_fingerprint: Dict[str, Any],
    fingerprint_window: Dict[str, Any],
    resolved_window: Dict[str, Any],
    candidate_sets: Dict[str, CandidateSet],
    selector_diagnostics: Dict[str, Any],
    cohort_diagnostics: Dict[str, Any],
    seed_vin_count: int,
    closed_vin_count: int,
    listing_count: int,
    artifact_count: int,
) -> Dict[str, Any]:
    return {
        "cache_schema_version": CACHE_SCHEMA_VERSION,
        "fingerprint": fingerprint,
        "request_fingerprint": request_fingerprint,
        "fingerprint_window": fingerprint_window,
        "resolved_window": resolved_window,
        "selector_candidates": serialize_candidate_sets(candidate_sets),
        "selector_diagnostics": selector_diagnostics,
        "cohort_diagnostics": cohort_diagnostics,
        "seed_vin_count": seed_vin_count,
        "closed_vin_count": closed_vin_count,
        "listing_count": listing_count,
        "artifact_count": artifact_count,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def load_planning_cache(path: str) -> Optional[Dict[str, Any]]:
    """Load a planning cache artifact, or None on a miss or load failure."""
    logger.info("lake_snapshot_planning_cache: lookup start path=%s", path)
    try:
        cached = read_json(path)
    except Exception as e:
        logger.warning("lake_snapshot_planning_cache: load failed path=%s error=%s", path, e)
        return None
    if cached is None:
        logger.info("lake_snapshot_planning_cache: miss path=%s", path)
        return None
    if cached.get("cache_schema_version") != CACHE_SCHEMA_VERSION:
        logger.warning(
            "lake_snapshot_planning_cache: schema mismatch path=%s cached_version=%s "
            "expected_version=%s; treating as miss",
            path, cached.get("cache_schema_version"), CACHE_SCHEMA_VERSION,
        )
        return None
    logger.info("lake_snapshot_planning_cache: hit path=%s", path)
    return cached


def write_planning_cache(path: str, artifact: Dict[str, Any]) -> None:
    """Persist a fully-computed planning cache artifact. Never raises."""
    t0 = time.monotonic()
    try:
        write_json(path, artifact)
        logger.info(
            "lake_snapshot_planning_cache: write ok path=%s elapsed_s=%.2f",
            path, time.monotonic() - t0,
        )
    except Exception as e:
        logger.warning(
            "lake_snapshot_planning_cache: write failed path=%s elapsed_s=%.2f error=%s",
            path, time.monotonic() - t0, e,
        )
