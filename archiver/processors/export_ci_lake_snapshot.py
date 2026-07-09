"""
CI lake snapshot exporter (Plan 120).

Generates production-derived, coherent, sanitized fixture archives for CI and
local development. This first pass implements the request/response contract,
tier defaults, and dry-run planning only — it does not yet read production
Parquet, allocate cohorts, or write/upload archives.

CLI:
    python -m archiver.processors.export_ci_lake_snapshot --tier ci --dry-run
"""
import argparse
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from archiver.processors.lake_snapshot_cohort import (
    build_snapshot_cohort,
    candidate_sets_to_selector_diagnostics,
    collect_all_selector_candidates,
    open_duckdb_connection,
)
from archiver.processors.lake_snapshot_export import materialize_filtered_tables
from archiver.processors.lake_snapshot_export_cache import (
    DEFAULT_EXPORT_PREFIX,
    build_export_manifest,
    compute_export_fingerprint,
    export_manifest_path,
    load_export_manifest,
    write_export_manifest,
)
from archiver.processors.lake_snapshot_planning_cache import (
    DEFAULT_PLANNING_CACHE_PREFIX,
    VALID_BUCKET_GRAINS,
    build_planning_cache_artifact,
    compute_planning_fingerprint,
    load_planning_cache,
    planning_cache_path,
    resolve_planning_window,
    subtract_months,
    write_planning_cache,
)
from archiver.processors.lake_snapshot_selectors import build_selector_registry, run_lake_selectors
from archiver.processors.lake_source_audit import audit_source_tables
from shared.logging_setup import configure_logging

logger = logging.getLogger("archiver")

VALID_TIERS = ("edge", "ci", "dev", "full")

_SNAPSHOT_ID_RE = re.compile(r"^adaptive-refresh-[A-Za-z0-9._-]+$")

# tier -> (target_vins, max_archive_mb)
TIER_DEFAULTS: Dict[str, Dict[str, Optional[int]]] = {
    "edge": {"target_vins": 100, "max_archive_mb": 50},
    "ci": {"target_vins": 5000, "max_archive_mb": 250},
    "dev": {"target_vins": 25000, "max_archive_mb": 1024},
    "full": {"target_vins": None, "max_archive_mb": None},
}


class SnapshotRequestError(ValueError):
    """Raised when a SnapshotRequest fails validation."""


@dataclass(frozen=True)
class SnapshotRequest:
    tier: str
    snapshot_id: Optional[str] = None
    target_vins: Optional[int] = None
    max_archive_mb: Optional[int] = None
    max_rows: Optional[int] = None
    source_window_start: Optional[datetime] = None
    source_window_end: Optional[datetime] = None
    source_window_months: Optional[int] = None
    min_selector_coverage: bool = True
    dry_run: bool = False
    audit_sources: bool = False
    run_selectors: bool = False
    build_cohort: bool = False
    source_base_path: Optional[str] = None
    reuse_planning_cache: bool = False
    refresh_planning_cache: bool = False
    planning_cache_bucket_grain: str = "week"
    planning_cache_prefix: str = DEFAULT_PLANNING_CACHE_PREFIX
    reuse_export_cache: bool = False
    refresh_export_cache: bool = False
    export_cache_prefix: str = DEFAULT_EXPORT_PREFIX


@dataclass(frozen=True)
class SnapshotResult:
    snapshot_id: str
    tier: str
    status: str
    source_window_start: Optional[str] = None
    source_window_end: Optional[str] = None
    seed_vin_count: Optional[int] = None
    closed_vin_count: Optional[int] = None
    listing_count: Optional[int] = None
    artifact_count: Optional[int] = None
    archive_bytes: Optional[int] = None
    manifest_key: Optional[str] = None
    archive_key: Optional[str] = None
    coverage_failures: List[str] = field(default_factory=list)
    source_audit: Optional[Dict[str, Any]] = None
    selector_diagnostics: Optional[Dict[str, Any]] = None
    cohort_diagnostics: Optional[Dict[str, Any]] = None
    planning_cache_key: Optional[str] = None
    planning_cache_path: Optional[str] = None
    planning_cache_hit: bool = False
    planning_cache_action: Optional[str] = None
    export_fingerprint: Optional[str] = None
    export_cache_hit: bool = False
    export_cache_action: Optional[str] = None
    materialized_snapshot_path: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "tier": self.tier,
            "status": self.status,
            "source_window_start": self.source_window_start,
            "source_window_end": self.source_window_end,
            "seed_vin_count": self.seed_vin_count,
            "closed_vin_count": self.closed_vin_count,
            "listing_count": self.listing_count,
            "artifact_count": self.artifact_count,
            "archive_bytes": self.archive_bytes,
            "manifest_key": self.manifest_key,
            "archive_key": self.archive_key,
            "coverage_failures": self.coverage_failures,
            "source_audit": self.source_audit,
            "selector_diagnostics": self.selector_diagnostics,
            "cohort_diagnostics": self.cohort_diagnostics,
            "planning_cache_key": self.planning_cache_key,
            "planning_cache_path": self.planning_cache_path,
            "planning_cache_hit": self.planning_cache_hit,
            "planning_cache_action": self.planning_cache_action,
            "export_fingerprint": self.export_fingerprint,
            "export_cache_hit": self.export_cache_hit,
            "export_cache_action": self.export_cache_action,
            "materialized_snapshot_path": self.materialized_snapshot_path,
        }


def validate_request(request: SnapshotRequest) -> None:
    """Raise SnapshotRequestError if the request is invalid."""
    if request.tier not in VALID_TIERS:
        raise SnapshotRequestError(
            f"Invalid tier '{request.tier}'; must be one of {VALID_TIERS}"
        )

    if request.snapshot_id is not None and not _SNAPSHOT_ID_RE.match(request.snapshot_id):
        raise SnapshotRequestError(
            f"Invalid snapshot_id '{request.snapshot_id}'; must match "
            f"adaptive-refresh-[A-Za-z0-9._-]+"
        )

    for field_name in ("target_vins", "max_archive_mb", "max_rows", "source_window_months"):
        value = getattr(request, field_name)
        if value is not None and value <= 0:
            raise SnapshotRequestError(f"{field_name} must be positive when set, got {value}")

    window_start = request.source_window_start
    window_end = request.source_window_end
    if (window_start is None) != (window_end is None):
        raise SnapshotRequestError(
            "source_window_start and source_window_end must both be null or both be set"
        )
    if window_start is not None and window_end is not None and window_start >= window_end:
        raise SnapshotRequestError(
            "source_window_start must be strictly before source_window_end"
        )

    if request.reuse_planning_cache and request.refresh_planning_cache:
        raise SnapshotRequestError(
            "reuse_planning_cache and refresh_planning_cache cannot both be set"
        )
    if request.planning_cache_bucket_grain not in VALID_BUCKET_GRAINS:
        raise SnapshotRequestError(
            f"Invalid planning_cache_bucket_grain '{request.planning_cache_bucket_grain}'; "
            f"must be one of {VALID_BUCKET_GRAINS}"
        )
    if request.reuse_export_cache and request.refresh_export_cache:
        raise SnapshotRequestError(
            "reuse_export_cache and refresh_export_cache cannot both be set"
        )


def resolve_request_defaults(request: SnapshotRequest) -> SnapshotRequest:
    """Fill in tier-derived target_vins/max_archive_mb when not explicitly set."""
    defaults = TIER_DEFAULTS[request.tier]
    target_vins = (
        request.target_vins if request.target_vins is not None else defaults["target_vins"]
    )
    max_archive_mb = (
        request.max_archive_mb
        if request.max_archive_mb is not None
        else defaults["max_archive_mb"]
    )
    if target_vins == request.target_vins and max_archive_mb == request.max_archive_mb:
        return request
    return SnapshotRequest(
        tier=request.tier,
        snapshot_id=request.snapshot_id,
        target_vins=target_vins,
        max_archive_mb=max_archive_mb,
        max_rows=request.max_rows,
        source_window_start=request.source_window_start,
        source_window_end=request.source_window_end,
        source_window_months=request.source_window_months,
        min_selector_coverage=request.min_selector_coverage,
        dry_run=request.dry_run,
        audit_sources=request.audit_sources,
        run_selectors=request.run_selectors,
        build_cohort=request.build_cohort,
        source_base_path=request.source_base_path,
        reuse_planning_cache=request.reuse_planning_cache,
        refresh_planning_cache=request.refresh_planning_cache,
        planning_cache_bucket_grain=request.planning_cache_bucket_grain,
        planning_cache_prefix=request.planning_cache_prefix,
        reuse_export_cache=request.reuse_export_cache,
        refresh_export_cache=request.refresh_export_cache,
        export_cache_prefix=request.export_cache_prefix,
    )


def generate_snapshot_id(tier: str, now: Optional[datetime] = None) -> str:
    now = now or datetime.now(timezone.utc)
    return f"adaptive-refresh-{now.strftime('%Y-%m-%d-%H%M%S')}"


def resolve_source_window(
    request: SnapshotRequest, now: Optional[datetime] = None
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """Resolve the effective source window from explicit start/end or months-back."""
    if request.source_window_start is not None and request.source_window_end is not None:
        return request.source_window_start, request.source_window_end

    if request.source_window_months is not None:
        now = now or datetime.now(timezone.utc)
        window_end = now
        window_start = subtract_months(now, request.source_window_months)
        return window_start, window_end

    return None, None


def build_manifest_skeleton(
    snapshot_id: str, request: SnapshotRequest, window_start, window_end
) -> Dict[str, Any]:
    """Build the initial (pre-generation) manifest shell for a snapshot."""
    return {
        "snapshot_id": snapshot_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_by": "archiver",
        "source": {
            "bucket": "bronze",
            "window_start": window_start.isoformat() if window_start else None,
            "window_end": window_end.isoformat() if window_end else None,
        },
        "tier": request.tier,
        "limits": {
            "target_vins": request.target_vins,
            "max_archive_mb": request.max_archive_mb,
            "max_rows": request.max_rows,
        },
        "counts": {
            "seed_vins": None,
            "closed_vins": None,
            "listing_ids": None,
            "artifact_ids": None,
        },
        "coverage": {},
        "tables": {},
        "archive": None,
        "generator": {
            "service": "archiver",
            "version": None,
            "selector_version": 1,
        },
    }


def format_coverage_failures(coverage: Dict[str, Dict[str, int]]) -> List[str]:
    """Format selector names whose entity count is below the required minimum."""
    failures = []
    for name, info in coverage.items():
        required = info.get("required", 0)
        entities = info.get("entities", 0)
        if entities < required:
            failures.append(
                f"{name}: found {entities}, required {required}"
            )
    return failures


@dataclass
class _PlanningResult:
    """Output of the heavy selector+cohort planning path, shared by the
    dry-run diagnostics branch and the real (non-dry-run) export path — a
    real export always needs a closed cohort, so it always runs this same
    planning regardless of the run_selectors/build_cohort request flags."""
    window_start: Optional[datetime]
    window_end: Optional[datetime]
    selector_diagnostics: Optional[Dict[str, Any]]
    coverage_failures: List[str]
    cohort_diagnostics: Optional[Dict[str, Any]]
    seed_vin_count: Optional[int]
    closed_vin_count: Optional[int]
    listing_count: Optional[int]
    artifact_count: Optional[int]
    cohort_seed_vins: Optional[set]
    cohort_closed_vins: Optional[set]
    cohort_listing_ids: Optional[set]
    cohort_artifact_ids: Optional[set]
    cohort_artifact_row_keys: Optional[set]
    cache_key: Optional[str]
    cache_path: Optional[str]
    cache_hit: bool
    cache_action: Optional[str]


def _run_heavy_planning(
    request: SnapshotRequest,
    snapshot_id: str,
    window_start: Optional[datetime],
    window_end: Optional[datetime],
    now: datetime,
) -> _PlanningResult:
    """Run (or reuse from cache) selector candidate collection + cohort
    allocation/closure — the expensive part of planning (Gate C.75).

    A relative (source_window_months) window is re-anchored to the bucketed
    "now" here so the query actually executed always matches the
    fingerprint's identity — otherwise two requests in the same bucket could
    hash the same fingerprint while querying different exact windows.
    """
    window_start, window_end = resolve_planning_window(
        request, window_start, window_end, now=now,
    )
    resolved_window = {
        "start": window_start.isoformat() if window_start else None,
        "end": window_end.isoformat() if window_end else None,
    }
    logger.info(
        "export_ci_lake_snapshot: planning_cache fingerprint compute start snapshot_id=%s",
        snapshot_id,
    )
    cache_key, request_fingerprint = compute_planning_fingerprint(
        request, window_start, window_end,
    )
    fingerprint_window = request_fingerprint["fingerprint_window"]
    cache_path = planning_cache_path(request.planning_cache_prefix, cache_key)

    logger.info(
        "export_ci_lake_snapshot: planning_cache lookup snapshot_id=%s "
        "fingerprint=%s reuse=%s refresh=%s path=%s",
        snapshot_id, cache_key, request.reuse_planning_cache,
        request.refresh_planning_cache, cache_path,
    )

    cached_artifact = None
    if request.reuse_planning_cache:
        cached_artifact = load_planning_cache(cache_path)

    cache_hit = cached_artifact is not None
    if cache_hit:
        cache_action = "reused"
        selector_diagnostics = cached_artifact["selector_diagnostics"]
        cohort_diagnostics = cached_artifact["cohort_diagnostics"]
        seed_vin_count = cached_artifact["seed_vin_count"]
        closed_vin_count = cached_artifact["closed_vin_count"]
        listing_count = cached_artifact["listing_count"]
        artifact_count = cached_artifact["artifact_count"]
        cohort_seed_vins = set(cached_artifact["seed_vins"])
        cohort_closed_vins = set(cached_artifact["closed_vins"])
        cohort_listing_ids = set(cached_artifact["listing_ids"])
        cohort_artifact_ids = set(cached_artifact["artifact_ids"])
        cohort_artifact_row_keys = {
            tuple(key) for key in cached_artifact["artifact_row_keys"]
        }
        coverage_failures: List[str] = []
        if request.min_selector_coverage:
            coverage_failures = format_coverage_failures(selector_diagnostics["selectors"])
        logger.info(
            "export_ci_lake_snapshot: planning_cache hit snapshot_id=%s fingerprint=%s",
            snapshot_id, cache_key,
        )
    else:
        cache_action = "refreshed" if request.refresh_planning_cache else "computed"
        # Collect selector candidates once and reuse them for both selector
        # diagnostics and cohort allocation, rather than scanning the lake
        # twice (run_lake_selectors + build_snapshot_cohort).
        con = open_duckdb_connection(request.source_base_path)
        try:
            candidate_sets = collect_all_selector_candidates(
                con, base_path=request.source_base_path,
                window_start=window_start, window_end=window_end,
            )
            selector_diagnostics = candidate_sets_to_selector_diagnostics(
                candidate_sets, request.source_base_path,
            )
            logger.info(
                "export_ci_lake_snapshot: run_selectors snapshot_id=%s tier=%s ok=%s errors=%s",
                snapshot_id, request.tier,
                selector_diagnostics["ok"], selector_diagnostics["errors"],
            )
            coverage_failures = []
            if request.min_selector_coverage:
                coverage_failures = format_coverage_failures(selector_diagnostics["selectors"])
            cohort = build_snapshot_cohort(
                con, request.source_base_path, window_start, window_end,
                request.target_vins, candidate_sets=candidate_sets,
            )
        finally:
            con.close()
        cohort_diagnostics = cohort.diagnostics
        seed_vin_count = len(cohort.seed_vins)
        closed_vin_count = len(cohort.closed_vins)
        listing_count = len(cohort.listing_ids)
        artifact_count = len(cohort.artifact_ids)
        cohort_seed_vins = cohort.seed_vins
        cohort_closed_vins = cohort.closed_vins
        cohort_listing_ids = cohort.listing_ids
        cohort_artifact_ids = cohort.artifact_ids
        cohort_artifact_row_keys = cohort.artifact_row_keys
        logger.info(
            "export_ci_lake_snapshot: build_cohort snapshot_id=%s tier=%s "
            "seed_vins=%s closed_vins=%s listing_ids=%s artifact_ids=%s",
            snapshot_id, request.tier, seed_vin_count, closed_vin_count,
            listing_count, artifact_count,
        )

        artifact = build_planning_cache_artifact(
            fingerprint=cache_key,
            request_fingerprint=request_fingerprint,
            fingerprint_window=fingerprint_window,
            resolved_window=resolved_window,
            candidate_sets=candidate_sets,
            selector_diagnostics=selector_diagnostics,
            cohort_diagnostics=cohort_diagnostics,
            seed_vins=cohort_seed_vins,
            closed_vins=cohort_closed_vins,
            listing_ids=cohort_listing_ids,
            artifact_ids=cohort_artifact_ids,
            artifact_row_keys=cohort_artifact_row_keys,
        )
        logger.info(
            "export_ci_lake_snapshot: planning_cache write start snapshot_id=%s "
            "fingerprint=%s path=%s",
            snapshot_id, cache_key, cache_path,
        )
        write_planning_cache(cache_path, artifact)

    return _PlanningResult(
        window_start=window_start,
        window_end=window_end,
        selector_diagnostics=selector_diagnostics,
        coverage_failures=coverage_failures,
        cohort_diagnostics=cohort_diagnostics,
        seed_vin_count=seed_vin_count,
        closed_vin_count=closed_vin_count,
        listing_count=listing_count,
        artifact_count=artifact_count,
        cohort_seed_vins=cohort_seed_vins,
        cohort_closed_vins=cohort_closed_vins,
        cohort_listing_ids=cohort_listing_ids,
        cohort_artifact_ids=cohort_artifact_ids,
        cohort_artifact_row_keys=cohort_artifact_row_keys,
        cache_key=cache_key,
        cache_path=cache_path,
        cache_hit=cache_hit,
        cache_action=cache_action,
    )


def export_ci_lake_snapshot(request: SnapshotRequest) -> SnapshotResult:
    """Run (or plan, for dry_run) a CI lake snapshot export."""
    validate_request(request)
    request = resolve_request_defaults(request)

    now = datetime.now(timezone.utc)
    snapshot_id = request.snapshot_id or generate_snapshot_id(request.tier, now=now)
    window_start, window_end = resolve_source_window(request, now=now)

    logger.info(
        "export_ci_lake_snapshot: request snapshot_id=%s tier=%s dry_run=%s "
        "audit_sources=%s run_selectors=%s build_cohort=%s source_window_start=%s "
        "source_window_end=%s target_vins=%s reuse_planning_cache=%s "
        "refresh_planning_cache=%s planning_cache_bucket_grain=%s",
        snapshot_id, request.tier, request.dry_run, request.audit_sources,
        request.run_selectors, request.build_cohort,
        window_start.isoformat() if window_start else None,
        window_end.isoformat() if window_end else None,
        request.target_vins, request.reuse_planning_cache,
        request.refresh_planning_cache, request.planning_cache_bucket_grain,
    )

    # Registry is built (and validated for unique names) even in this
    # scaffolding pass, so selector shape is exercised end-to-end.
    build_selector_registry()

    if request.audit_sources:
        source_audit = audit_source_tables(
            base_path=request.source_base_path,
            window_start=window_start,
            window_end=window_end,
        )
        logger.info(
            "export_ci_lake_snapshot: audit_sources snapshot_id=%s tier=%s ok=%s errors=%s",
            snapshot_id, request.tier, source_audit["ok"], source_audit["errors"],
        )
        return SnapshotResult(
            snapshot_id=snapshot_id,
            tier=request.tier,
            status="audited",
            source_window_start=window_start.isoformat() if window_start else None,
            source_window_end=window_end.isoformat() if window_end else None,
            coverage_failures=[],
            source_audit=source_audit,
        )

    if request.dry_run:
        logger.info(
            "export_ci_lake_snapshot: dry_run snapshot_id=%s tier=%s target_vins=%s "
            "max_archive_mb=%s run_selectors=%s",
            snapshot_id, request.tier, request.target_vins, request.max_archive_mb,
            request.run_selectors,
        )
        selector_diagnostics = None
        coverage_failures: List[str] = []
        cohort_diagnostics = None
        seed_vin_count = closed_vin_count = listing_count = artifact_count = None
        cache_key = cache_path = None
        cache_hit = False
        cache_action = None
        if request.run_selectors and request.build_cohort:
            planning = _run_heavy_planning(request, snapshot_id, window_start, window_end, now)
            window_start, window_end = planning.window_start, planning.window_end
            selector_diagnostics = planning.selector_diagnostics
            coverage_failures = planning.coverage_failures
            cohort_diagnostics = planning.cohort_diagnostics
            seed_vin_count = planning.seed_vin_count
            closed_vin_count = planning.closed_vin_count
            listing_count = planning.listing_count
            artifact_count = planning.artifact_count
            cache_key = planning.cache_key
            cache_path = planning.cache_path
            cache_hit = planning.cache_hit
            cache_action = planning.cache_action
        elif request.run_selectors:
            selector_diagnostics = run_lake_selectors(
                base_path=request.source_base_path,
                window_start=window_start,
                window_end=window_end,
            )
            logger.info(
                "export_ci_lake_snapshot: run_selectors snapshot_id=%s tier=%s ok=%s errors=%s",
                snapshot_id, request.tier,
                selector_diagnostics["ok"], selector_diagnostics["errors"],
            )
            if request.min_selector_coverage:
                coverage_failures = format_coverage_failures(selector_diagnostics["selectors"])
        return SnapshotResult(
            snapshot_id=snapshot_id,
            tier=request.tier,
            status="planned",
            source_window_start=window_start.isoformat() if window_start else None,
            source_window_end=window_end.isoformat() if window_end else None,
            seed_vin_count=seed_vin_count,
            closed_vin_count=closed_vin_count,
            listing_count=listing_count,
            artifact_count=artifact_count,
            coverage_failures=coverage_failures,
            selector_diagnostics=selector_diagnostics,
            cohort_diagnostics=cohort_diagnostics,
            planning_cache_key=cache_key,
            planning_cache_path=cache_path,
            planning_cache_hit=cache_hit,
            planning_cache_action=cache_action,
        )

    # Non-dry-run: a real export always needs a closed cohort, so it always
    # runs the same heavy planning as the dry-run run_selectors+build_cohort
    # branch, regardless of those request flags (they only gate dry-run
    # diagnostics scope).
    planning = _run_heavy_planning(request, snapshot_id, window_start, window_end, now)
    window_start, window_end = planning.window_start, planning.window_end

    if request.min_selector_coverage and planning.coverage_failures:
        logger.info(
            "export_ci_lake_snapshot: export blocked by coverage_failures snapshot_id=%s "
            "failures=%s",
            snapshot_id, planning.coverage_failures,
        )
        return SnapshotResult(
            snapshot_id=snapshot_id,
            tier=request.tier,
            status="coverage_failed",
            source_window_start=window_start.isoformat() if window_start else None,
            source_window_end=window_end.isoformat() if window_end else None,
            seed_vin_count=planning.seed_vin_count,
            closed_vin_count=planning.closed_vin_count,
            listing_count=planning.listing_count,
            artifact_count=planning.artifact_count,
            coverage_failures=planning.coverage_failures,
            selector_diagnostics=planning.selector_diagnostics,
            cohort_diagnostics=planning.cohort_diagnostics,
            planning_cache_key=planning.cache_key,
            planning_cache_path=planning.cache_path,
            planning_cache_hit=planning.cache_hit,
            planning_cache_action=planning.cache_action,
        )

    export_fingerprint, export_fingerprint_payload = compute_export_fingerprint(planning.cache_key)
    export_manifest_key = export_manifest_path(request.export_cache_prefix, export_fingerprint)

    def _export_failed(reason_failures: List[str], export_cache_action: str) -> SnapshotResult:
        return SnapshotResult(
            snapshot_id=snapshot_id,
            tier=request.tier,
            status="export_failed",
            source_window_start=window_start.isoformat() if window_start else None,
            source_window_end=window_end.isoformat() if window_end else None,
            seed_vin_count=planning.seed_vin_count,
            closed_vin_count=planning.closed_vin_count,
            listing_count=planning.listing_count,
            artifact_count=planning.artifact_count,
            coverage_failures=reason_failures,
            selector_diagnostics=planning.selector_diagnostics,
            cohort_diagnostics=planning.cohort_diagnostics,
            planning_cache_key=planning.cache_key,
            planning_cache_path=planning.cache_path,
            planning_cache_hit=planning.cache_hit,
            planning_cache_action=planning.cache_action,
            export_fingerprint=export_fingerprint,
            export_cache_hit=False,
            export_cache_action=export_cache_action,
        )

    logger.info(
        "export_ci_lake_snapshot: export_cache lookup snapshot_id=%s export_fingerprint=%s "
        "reuse=%s refresh=%s path=%s",
        snapshot_id, export_fingerprint, request.reuse_export_cache,
        request.refresh_export_cache, export_manifest_key,
    )
    cached_manifest = None
    if request.reuse_export_cache:
        cached_manifest = load_export_manifest(export_manifest_key, export_fingerprint)

    export_cache_hit = cached_manifest is not None
    if export_cache_hit:
        export_cache_action = "reused"
        materialized_snapshot_path = cached_manifest["data_path"]
        logger.info(
            "export_ci_lake_snapshot: export_cache hit snapshot_id=%s export_fingerprint=%s",
            snapshot_id, export_fingerprint,
        )
    else:
        export_cache_action = "refreshed" if request.refresh_export_cache else "computed"
        con = open_duckdb_connection(request.source_base_path)
        try:
            result = materialize_filtered_tables(
                con, request.source_base_path, window_start, window_end,
                planning.cohort_closed_vins, planning.cohort_listing_ids,
                planning.cohort_artifact_row_keys,
                export_fingerprint, request.export_cache_prefix,
            )
        finally:
            con.close()

        if not result.ok:
            # materialize_filtered_tables already discarded the failed
            # generation directory — never write/publish a manifest for a
            # partial result, or a later reuse_export_cache request would
            # accept an incomplete snapshot as valid.
            table_errors = {
                name: t["error"] for name, t in result.tables.items() if t["error"]
            }
            logger.warning(
                "export_ci_lake_snapshot: export failed snapshot_id=%s "
                "export_fingerprint=%s table_errors=%s",
                snapshot_id, export_fingerprint, table_errors,
            )
            return _export_failed(
                [f"{name}: {err}" for name, err in table_errors.items()], export_cache_action,
            )

        manifest = build_export_manifest(
            fingerprint=export_fingerprint,
            planning_fingerprint=planning.cache_key,
            export_fingerprint_payload=export_fingerprint_payload,
            snapshot_id=snapshot_id,
            tier=request.tier,
            source_window={
                "start": window_start.isoformat() if window_start else None,
                "end": window_end.isoformat() if window_end else None,
            },
            counts={
                "seed_vins": planning.seed_vin_count,
                "closed_vins": planning.closed_vin_count,
                "listing_ids": planning.listing_count,
                "artifact_ids": planning.artifact_count,
            },
            coverage=(
                planning.selector_diagnostics["selectors"]
                if planning.selector_diagnostics else {}
            ),
            tables=result.tables,
            data_path=result.data_path,
            generation_id=result.generation_id,
        )
        manifest_written = write_export_manifest(export_manifest_key, manifest)
        if not manifest_written:
            # The manifest write is the actual "publish" step — a fully
            # materialized generation that never got a manifest pointing at
            # it is not reusable and must not be reported as exported.
            logger.warning(
                "export_ci_lake_snapshot: manifest write failed snapshot_id=%s "
                "export_fingerprint=%s path=%s",
                snapshot_id, export_fingerprint, export_manifest_key,
            )
            return _export_failed(
                [f"manifest write failed: {export_manifest_key}"], export_cache_action,
            )
        materialized_snapshot_path = result.data_path

    return SnapshotResult(
        snapshot_id=snapshot_id,
        tier=request.tier,
        status="exported",
        source_window_start=window_start.isoformat() if window_start else None,
        source_window_end=window_end.isoformat() if window_end else None,
        seed_vin_count=planning.seed_vin_count,
        closed_vin_count=planning.closed_vin_count,
        listing_count=planning.listing_count,
        artifact_count=planning.artifact_count,
        coverage_failures=planning.coverage_failures,
        selector_diagnostics=planning.selector_diagnostics,
        cohort_diagnostics=planning.cohort_diagnostics,
        manifest_key=export_manifest_key,
        planning_cache_key=planning.cache_key,
        planning_cache_path=planning.cache_path,
        planning_cache_hit=planning.cache_hit,
        planning_cache_action=planning.cache_action,
        export_fingerprint=export_fingerprint,
        export_cache_hit=export_cache_hit,
        export_cache_action=export_cache_action,
        materialized_snapshot_path=materialized_snapshot_path,
    )


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export a CI lake snapshot")
    parser.add_argument("--tier", choices=VALID_TIERS, required=True)
    parser.add_argument("--snapshot-id", dest="snapshot_id", default=None)
    parser.add_argument("--target-vins", dest="target_vins", type=int, default=None)
    parser.add_argument("--max-archive-mb", dest="max_archive_mb", type=int, default=None)
    parser.add_argument("--max-rows", dest="max_rows", type=int, default=None)
    parser.add_argument(
        "--source-window-months", dest="source_window_months", type=int, default=None
    )
    parser.add_argument("--dry-run", dest="dry_run", action="store_true")
    parser.add_argument("--audit-sources", dest="audit_sources", action="store_true")
    parser.add_argument("--run-selectors", dest="run_selectors", action="store_true")
    parser.add_argument("--build-cohort", dest="build_cohort", action="store_true")
    parser.add_argument("--source-base-path", dest="source_base_path", default=None)
    parser.add_argument(
        "--reuse-planning-cache", dest="reuse_planning_cache", action="store_true"
    )
    parser.add_argument(
        "--refresh-planning-cache", dest="refresh_planning_cache", action="store_true"
    )
    parser.add_argument(
        "--planning-cache-bucket-grain", dest="planning_cache_bucket_grain",
        choices=VALID_BUCKET_GRAINS, default="week",
    )
    parser.add_argument(
        "--planning-cache-prefix", dest="planning_cache_prefix",
        default=DEFAULT_PLANNING_CACHE_PREFIX,
    )
    parser.add_argument(
        "--reuse-export-cache", dest="reuse_export_cache", action="store_true"
    )
    parser.add_argument(
        "--refresh-export-cache", dest="refresh_export_cache", action="store_true"
    )
    parser.add_argument(
        "--export-cache-prefix", dest="export_cache_prefix", default=DEFAULT_EXPORT_PREFIX,
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    configure_logging()
    args = _parse_args(argv)
    request = SnapshotRequest(
        tier=args.tier,
        snapshot_id=args.snapshot_id,
        target_vins=args.target_vins,
        max_archive_mb=args.max_archive_mb,
        max_rows=args.max_rows,
        source_window_months=args.source_window_months,
        dry_run=args.dry_run,
        audit_sources=args.audit_sources,
        run_selectors=args.run_selectors,
        build_cohort=args.build_cohort,
        source_base_path=args.source_base_path,
        reuse_planning_cache=args.reuse_planning_cache,
        refresh_planning_cache=args.refresh_planning_cache,
        planning_cache_bucket_grain=args.planning_cache_bucket_grain,
        planning_cache_prefix=args.planning_cache_prefix,
        reuse_export_cache=args.reuse_export_cache,
        refresh_export_cache=args.refresh_export_cache,
        export_cache_prefix=args.export_cache_prefix,
    )
    result = export_ci_lake_snapshot(request)
    print(json.dumps(result.to_dict(), indent=2))


if __name__ == "__main__":
    main()
