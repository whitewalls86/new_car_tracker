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

from archiver.processors.lake_snapshot_selectors import build_selector_registry

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

    for field_name in ("target_vins", "max_archive_mb", "max_rows"):
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
        window_start = _subtract_months(now, request.source_window_months)
        return window_start, window_end

    return None, None


def _subtract_months(dt: datetime, months: int) -> datetime:
    total_months = dt.month - 1 - months
    year = dt.year + total_months // 12
    month = total_months % 12 + 1
    day = min(dt.day, _days_in_month(year, month))
    return dt.replace(year=year, month=month, day=day)


def _days_in_month(year: int, month: int) -> int:
    import calendar
    return calendar.monthrange(year, month)[1]


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


def export_ci_lake_snapshot(request: SnapshotRequest) -> SnapshotResult:
    """Run (or plan, for dry_run) a CI lake snapshot export."""
    validate_request(request)
    request = resolve_request_defaults(request)

    snapshot_id = request.snapshot_id or generate_snapshot_id(request.tier)
    window_start, window_end = resolve_source_window(request)

    # Registry is built (and validated for unique names) even in this
    # scaffolding pass, so selector shape is exercised end-to-end.
    build_selector_registry()

    if request.dry_run:
        logger.info(
            "export_ci_lake_snapshot: dry_run snapshot_id=%s tier=%s target_vins=%s "
            "max_archive_mb=%s",
            snapshot_id, request.tier, request.target_vins, request.max_archive_mb,
        )
        return SnapshotResult(
            snapshot_id=snapshot_id,
            tier=request.tier,
            status="planned",
            source_window_start=window_start.isoformat() if window_start else None,
            source_window_end=window_end.isoformat() if window_end else None,
            coverage_failures=[],
        )

    logger.info(
        "export_ci_lake_snapshot: non-dry-run export requested for snapshot_id=%s tier=%s "
        "but full export is not implemented yet",
        snapshot_id, request.tier,
    )
    return SnapshotResult(
        snapshot_id=snapshot_id,
        tier=request.tier,
        status="not_implemented",
        source_window_start=window_start.isoformat() if window_start else None,
        source_window_end=window_end.isoformat() if window_end else None,
        coverage_failures=[],
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
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = _parse_args(argv)
    request = SnapshotRequest(
        tier=args.tier,
        snapshot_id=args.snapshot_id,
        target_vins=args.target_vins,
        max_archive_mb=args.max_archive_mb,
        max_rows=args.max_rows,
        source_window_months=args.source_window_months,
        dry_run=args.dry_run,
    )
    result = export_ci_lake_snapshot(request)
    print(json.dumps(result.to_dict(), indent=2))


if __name__ == "__main__":
    main()
