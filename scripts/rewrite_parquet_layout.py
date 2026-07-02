"""Rewrite silver and ops Parquet from current layout into normalized month-level layout.

Source layouts (input):
  silver: silver/observations/source=X/obs_year=Y/obs_month=M/obs_day=D/*.parquet
  ops:    ops/<table>/year=Y/month=M/*.parquet

Target normalized layouts (output):
  silver: silver_normalized/observations/source=X/obs_year=Y/obs_month=M/part-<uuid>-0.parquet
  ops:    ops_normalized/<table>/year=Y/month=M/part-<uuid>-0.parquet

Default mode: dry-run (no writes). Pass --apply to write.
Never deletes or modifies old source-prefix files (silver/observations/, ops/).
In --replace-existing-target mode, deletes old normalized target files
(silver_normalized/, ops_normalized/) after the new file has been validated
and renamed. Any delete failure fails the unit and exits nonzero.

Usage:
  python scripts/rewrite_parquet_layout.py --all --dry-run
  python scripts/rewrite_parquet_layout.py --dataset silver_observations \\
      --source detail --month 2026-06 --apply \\
      --baseline-audit /tmp/audit.json --json-out /tmp/rewrite.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from shared.minio import BUCKET, get_boto3_client, get_s3fs

LOG = logging.getLogger("rewrite_parquet_layout")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SUPPORTED_DATASETS = [
    "silver_observations",
    "price_observation_events",
    "vin_to_listing_events",
    "blocked_cooldown_events",
    "detail_scrape_claim_events",
    "artifacts_queue_events",
]

SILVER_SOURCES = {"detail", "carousel", "srp"}

# Keys under silver/observations that match the old day-partitioned layout.
_SILVER_KEY_RE = re.compile(
    r"^silver/observations/source=([^/]+)"
    r"/obs_year=(\d+)/obs_month=(\d+)/obs_day=\d+/[^/]+\.parquet$"
)

# Hive partition columns that live in the path and must be excluded from the physical
# Parquet schema. Fresh flush files never include these physically; historical rewrites
# must match that behaviour so all files in a partition share one schema.
_SILVER_PARTITION_COLS: list[str] = ["source", "obs_year", "obs_month"]
_OPS_PARTITION_COLS: list[str] = ["year", "month"]

# Sort column priority per dataset; only columns actually present in the table are used.
_SORT_COLS: dict[str, list[str]] = {
    "silver_observations":        ["fetched_at", "listing_id", "artifact_id"],
    "price_observation_events":   ["event_at", "listing_id", "artifact_id", "event_id"],
    "vin_to_listing_events":      ["event_at", "listing_id", "artifact_id", "event_id"],
    "blocked_cooldown_events":    ["event_at", "listing_id", "event_id"],
    "detail_scrape_claim_events": ["event_at", "run_id", "listing_id", "event_id"],
    "artifacts_queue_events":     ["event_at", "fetched_at", "run_id", "artifact_id", "event_id"],
}

_TS_COL_CANDIDATES = frozenset({"fetched_at", "event_at"})

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class RewriteUnit:
    dataset: str
    source: Optional[str]   # silver only; None for ops
    year: int
    month: int
    source_prefix: str      # conceptual prefix covering all source files for this unit
    target_prefix: str      # normalized output prefix
    source_keys: list[str] = field(default_factory=list)
    source_bytes: int = 0


@dataclass
class UnitResult:
    dataset: str
    source: Optional[str]
    year: int
    month: int
    source_prefix: str
    target_prefix: str
    source_files: int
    source_bytes: int
    rows_source: Optional[int] = None
    rows_written: Optional[int] = None
    ts_min: Optional[str] = None
    ts_max: Optional[str] = None
    schema_fingerprint: Optional[str] = None
    status: str = "skipped"   # ok | skipped | failed
    error: Optional[str] = None
    target_file: Optional[str] = None
    baseline_mismatch: Optional[str] = None
    replaced_files: int = 0   # number of old normalized files deleted (replace mode only)


# ---------------------------------------------------------------------------
# Pure helpers (no I/O)
# ---------------------------------------------------------------------------


def _schema_fingerprint(schema: pa.Schema) -> str:
    """Short hash identifying a schema by sorted field names+types.

    Column reordering does not produce a different fingerprint.
    """
    canonical = ",".join(
        f"{f.name}:{f.type}" for f in sorted(schema, key=lambda f: f.name)
    )
    return hashlib.md5(canonical.encode()).hexdigest()[:12]  # noqa: S324


def _extract_ts_range(table: pa.Table) -> tuple[Optional[str], Optional[str]]:
    """Return ISO-8601 min/max from the first available timestamp column."""
    ts_col = next(
        (
            c for c in _TS_COL_CANDIDATES
            if c in table.schema.names
            and pa.types.is_timestamp(table.schema.field(c).type)
        ),
        None,
    )
    if ts_col is None:
        return None, None
    col = table.column(ts_col).cast(pa.timestamp("us", tz="UTC"))
    mn = pc.min(col).as_py()
    mx = pc.max(col).as_py()
    if mn is None or mx is None:
        return None, None

    def _iso(dt: datetime) -> str:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()

    return _iso(mn), _iso(mx)


def _drop_partition_cols(table: pa.Table, dataset: str) -> pa.Table:
    """Select away Hive partition columns so they live only in the path, not physically.

    This makes rewritten files schema-identical to fresh flush files, which rely
    on partition-path inference and never include these columns physically.
    """
    to_drop = (
        _SILVER_PARTITION_COLS if dataset == "silver_observations" else _OPS_PARTITION_COLS
    )
    keep = [name for name in table.schema.names if name not in to_drop]
    if len(keep) == len(table.schema.names):
        return table
    return table.select(keep)


def _concat_sort(tables: list[pa.Table], sort_cols: list[str]) -> pa.Table:
    """Concat a list of Arrow tables and sort by available columns.

    Only columns that exist in the combined schema are used for sorting.
    Missing sort columns are silently skipped — they do not raise.
    """
    combined = pa.concat_tables(tables)
    available = [c for c in sort_cols if c in combined.schema.names]
    if available:
        combined = combined.sort_by([(c, "ascending") for c in available])
    return combined


# ---------------------------------------------------------------------------
# Listing helpers (boto3 pagination, same pattern as audit_parquet_layout)
# ---------------------------------------------------------------------------


def iter_objects(client, bucket: str, prefix: str):
    """Yield (key, size) for every object under prefix (streaming pagination)."""
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for entry in page.get("Contents", []):
            yield entry["Key"], entry["Size"]


def _list_target_parquet(fs, bucket: str, target_prefix: str) -> list[str]:
    """Return .parquet (not .tmp) keys in target_prefix for skip-existing check.

    Returns an empty list if the prefix does not exist.
    """
    try:
        entries = fs.ls(f"{bucket}/{target_prefix}", detail=False)
    except FileNotFoundError:
        return []
    # s3fs returns paths like "bucket/prefix/key"; filter by extension only.
    # ".parquet.tmp" ends in ".tmp" not ".parquet", so endswith(".parquet") is
    # already sufficient — the explicit .tmp exclusion makes the intent clear.
    return [
        e for e in entries
        if e.endswith(".parquet") and not e.endswith(".parquet.tmp")
    ]


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def _ops_source_re(table: str) -> re.Pattern:
    return re.compile(
        rf"^ops/{re.escape(table)}/year=(\d+)/month=(\d+)/[^/]+\.parquet$"
    )


_OPS_SOURCE_RES: dict[str, re.Pattern] = {
    t: _ops_source_re(t) for t in SUPPORTED_DATASETS if t != "silver_observations"
}


def _discover_silver_units(
    client,
    bucket: str,
    source_filter: Optional[str],
    month_filter: Optional[tuple[int, int]],
) -> list[RewriteUnit]:
    units_map: dict[tuple[str, int, int], RewriteUnit] = {}
    for key, size in iter_objects(client, bucket, "silver/observations/"):
        m = _SILVER_KEY_RE.match(key)
        if not m:
            continue
        source, year, month = m.group(1), int(m.group(2)), int(m.group(3))
        if source_filter and source != source_filter:
            continue
        if month_filter and (year, month) != month_filter:
            continue
        uk = (source, year, month)
        if uk not in units_map:
            units_map[uk] = RewriteUnit(
                dataset="silver_observations",
                source=source,
                year=year,
                month=month,
                source_prefix=(
                    f"silver/observations/source={source}"
                    f"/obs_year={year}/obs_month={month}/"
                ),
                target_prefix=(
                    f"silver_normalized/observations/source={source}"
                    f"/obs_year={year}/obs_month={month}/"
                ),
            )
        units_map[uk].source_keys.append(key)
        units_map[uk].source_bytes += size
    return sorted(units_map.values(), key=lambda u: (u.source, u.year, u.month))


def _discover_ops_units(
    client,
    bucket: str,
    table: str,
    month_filter: Optional[tuple[int, int]],
) -> list[RewriteUnit]:
    re_ = _OPS_SOURCE_RES[table]
    units_map: dict[tuple[int, int], RewriteUnit] = {}
    for key, size in iter_objects(client, bucket, f"ops/{table}/"):
        m = re_.match(key)
        if not m:
            continue
        year, month = int(m.group(1)), int(m.group(2))
        if month_filter and (year, month) != month_filter:
            continue
        uk = (year, month)
        if uk not in units_map:
            units_map[uk] = RewriteUnit(
                dataset=table,
                source=None,
                year=year,
                month=month,
                source_prefix=f"ops/{table}/year={year}/month={month}/",
                target_prefix=f"ops_normalized/{table}/year={year}/month={month}/",
            )
        units_map[uk].source_keys.append(key)
        units_map[uk].source_bytes += size
    return sorted(units_map.values(), key=lambda u: (u.year, u.month))


def discover_units(
    client,
    bucket: str,
    datasets: list[str],
    month_filter: Optional[tuple[int, int]] = None,
    source_filter: Optional[str] = None,
    limit_partitions: Optional[int] = None,
) -> list[RewriteUnit]:
    """List source objects and group them into rewrite units.

    One unit = one (source, year, month) for silver, or one (year, month)
    for ops tables. Each unit is rewritten as a single normalized Parquet file.
    """
    all_units: list[RewriteUnit] = []
    for dataset in datasets:
        if dataset == "silver_observations":
            units = _discover_silver_units(client, bucket, source_filter, month_filter)
        else:
            units = _discover_ops_units(client, bucket, dataset, month_filter)
        all_units.extend(units)
    if limit_partitions is not None:
        all_units = all_units[:limit_partitions]
    return all_units


# ---------------------------------------------------------------------------
# Baseline audit cross-check
# ---------------------------------------------------------------------------

_SILVER_PART_RE = re.compile(
    r"silver/observations/source=([^/]+)/obs_year=(\d+)/obs_month=(\d+)/obs_day=\d+/"
)
_OPS_PART_RE = re.compile(r"ops/([^/]+)/year=(\d+)/month=(\d+)/")


def load_baseline_audit(path: Path) -> dict[str, dict]:
    """Load a Phase 3 audit JSON and aggregate per-unit row counts.

    Returns:
        {
          "silver_observations": {(source, year, month): rows, ...},
          "<ops_table>":         {(year, month): rows, ...},
          ...
        }

    Partition rows from the Phase 3 audit are summed across all day partitions
    for the same source+month (silver) or year+month (ops).
    """
    data = json.loads(path.read_text())
    result: dict[str, dict] = {}
    for dataset_name, ds_data in data.get("datasets", {}).items():
        agg: dict = {}
        for part in ds_data.get("partitions", []):
            rows = part.get("rows")
            if rows is None:
                continue
            path_str = part["path"]
            if dataset_name == "silver_observations":
                m = _SILVER_PART_RE.search(path_str)
                if m:
                    key = (m.group(1), int(m.group(2)), int(m.group(3)))
                    agg[key] = agg.get(key, 0) + rows
            else:
                m = _OPS_PART_RE.search(path_str)
                if m:
                    key = (int(m.group(2)), int(m.group(3)))
                    agg[key] = agg.get(key, 0) + rows
        result[dataset_name] = agg
    return result


def _baseline_lookup(baseline: dict, unit: RewriteUnit) -> Optional[int]:
    """Return the expected row count from baseline for this unit, or None."""
    ds_baseline = baseline.get(unit.dataset)
    if ds_baseline is None:
        return None
    if unit.dataset == "silver_observations":
        return ds_baseline.get((unit.source, unit.year, unit.month))
    return ds_baseline.get((unit.year, unit.month))


# ---------------------------------------------------------------------------
# Apply logic (writes to normalized prefix)
# ---------------------------------------------------------------------------


def _apply_unit(
    unit: RewriteUnit,
    fs,
    bucket: str,
    baseline_rows: Optional[int] = None,
    replace_existing: bool = False,
) -> UnitResult:
    """Apply rewrite for one unit. Returns a UnitResult.

    Write sequence (mirrors Plan 109 safety pattern):
      1. Check skip-existing in target prefix (skipped when replace_existing=True).
      2. Read all source keys into memory.
      3. Concat + sort by available sort columns.
      4. Baseline cross-check:
           Normal mode: fail if source_rows != baseline_rows (exact match).
           Replace mode: fail if source_rows < baseline_rows (regression guard;
             delta rows from the final flush are expected to increase the count).
      5. Write to <target_prefix>part-<uuid>-0.parquet.tmp
         (.tmp extension is invisible to *.parquet glob readers).
      6. Read back metadata row count; abort if it does not equal source count.
      7. Rename .tmp → final .parquet.
      8. Replace mode only: delete the previous normalized files in target_prefix.
         Old-prefix files (silver/observations/, ops/) are never touched.
    """
    result = UnitResult(
        dataset=unit.dataset,
        source=unit.source,
        year=unit.year,
        month=unit.month,
        source_prefix=unit.source_prefix,
        target_prefix=unit.target_prefix,
        source_files=len(unit.source_keys),
        source_bytes=unit.source_bytes,
    )

    if not unit.source_keys:
        result.status = "skipped"
        result.error = "no source files"
        return result

    existing = _list_target_parquet(fs, bucket, unit.target_prefix)
    if existing and not replace_existing:
        # Skip-existing: avoid duplicate output files on reruns.
        LOG.info(
            "skip-existing: %s already has %d parquet file(s) — skipping",
            unit.target_prefix, len(existing),
        )
        result.status = "skipped"
        return result
    elif existing:
        LOG.info(
            "replace-existing: %s has %d file(s) — will replace after validated write",
            unit.target_prefix, len(existing),
        )

    # Read all source files and concat.
    sort_cols = _SORT_COLS.get(unit.dataset, [])
    tables: list[pa.Table] = []
    try:
        for key in unit.source_keys:
            t = pq.read_table(f"s3://{bucket}/{key}", filesystem=fs)
            tables.append(t)
    except Exception as exc:
        LOG.error(
            "read failed for unit %s %s %d-%02d: %s",
            unit.dataset, unit.source or "-", unit.year, unit.month, exc,
        )
        result.status = "failed"
        result.error = f"read error: {exc}"
        return result

    combined = _concat_sort(tables, sort_cols)
    combined = _drop_partition_cols(combined, unit.dataset)
    rows_source = len(combined)
    result.rows_source = rows_source
    result.schema_fingerprint = _schema_fingerprint(combined.schema)
    result.ts_min, result.ts_max = _extract_ts_range(combined)

    # Baseline cross-check.
    # Normal mode: exact match required (Phase 5). Any discrepancy means the source
    #   has changed since the audit; the operator should investigate before writing.
    # Replace mode: regression guard only (Phase 6 delta). The final flush adds rows,
    #   so source_rows > baseline_rows is expected and correct. Fail only if
    #   source_rows < baseline_rows, which would indicate data loss.
    if baseline_rows is not None:
        if replace_existing:
            if rows_source < baseline_rows:
                msg = (
                    f"baseline regression: source_rows={rows_source} "
                    f"< baseline_rows={baseline_rows} (data loss suspected)"
                )
                LOG.error(
                    "unit %s %s %d-%02d: %s",
                    unit.dataset, unit.source or "-", unit.year, unit.month, msg,
                )
                result.baseline_mismatch = msg
                result.status = "failed"
                result.error = msg
                return result
        else:
            if rows_source != baseline_rows:
                msg = (
                    f"baseline mismatch: source_rows={rows_source} "
                    f"baseline_rows={baseline_rows}"
                )
                LOG.error(
                    "unit %s %s %d-%02d: %s",
                    unit.dataset, unit.source or "-", unit.year, unit.month, msg,
                )
                result.baseline_mismatch = msg
                result.status = "failed"
                result.error = msg
                return result

    # Write to .parquet.tmp first. The .tmp extension does not match *.parquet
    # globs, so concurrent dbt/DuckDB readers cannot see a partially-written file.
    uid = str(uuid.uuid4())
    final_key = f"{unit.target_prefix}part-{uid}-0.parquet"
    tmp_key = f"{final_key}.tmp"

    try:
        pq.write_table(
            combined,
            f"s3://{bucket}/{tmp_key}",
            filesystem=fs,
            compression="zstd",
        )
    except Exception as exc:
        LOG.error("write failed for tmp %s: %s", tmp_key, exc)
        result.status = "failed"
        result.error = f"write error: {exc}"
        return result

    # Validate row count from written file metadata before rename.
    try:
        written_meta = pq.read_metadata(f"s3://{bucket}/{tmp_key}", filesystem=fs)
        rows_written = sum(
            written_meta.row_group(i).num_rows
            for i in range(written_meta.num_row_groups)
        )
    except Exception as exc:
        LOG.error("post-write metadata read failed for %s: %s", tmp_key, exc)
        result.status = "failed"
        result.error = (
            f"post-write metadata error: {exc}; "
            f"tmp left at {bucket}/{tmp_key} for manual recovery"
        )
        return result

    result.rows_written = rows_written

    if rows_written != rows_source:
        msg = (
            f"row count mismatch after write: "
            f"source={rows_source} written={rows_written}"
        )
        LOG.error(
            "%s — tmp left at %s/%s for manual recovery", msg, bucket, tmp_key
        )
        result.status = "failed"
        result.error = f"{msg}; tmp left at {bucket}/{tmp_key}"
        return result

    # Rename .tmp → final. If rename fails, leave tmp in place for manual recovery.
    try:
        fs.rename(f"{bucket}/{tmp_key}", f"{bucket}/{final_key}")
    except Exception as exc:
        LOG.error(
            "rename failed: %s → %s: %s — tmp left for manual recovery",
            tmp_key, final_key, exc,
        )
        result.status = "failed"
        result.error = (
            f"rename error: {exc}; "
            f"tmp left at {bucket}/{tmp_key} for manual recovery"
        )
        return result

    # Replace mode: remove old normalized files now that the new file is live.
    # There is a brief window after rename and before these deletes where readers
    # see both the old and new file simultaneously; this is acceptable during the
    # Phase 6 cutover (DAGs paused, deploy intent set, minimal reader traffic).
    # Old-prefix files (silver/observations/, ops/) are never touched here.
    if replace_existing and existing:
        failed_deletes: list[str] = []
        for old_path in existing:
            try:
                fs.rm(old_path)
            except Exception as exc:
                LOG.error(
                    "failed to delete old normalized file %s: %s — "
                    "target prefix now has both old and new files; "
                    "manual cleanup required to avoid double-counting",
                    old_path, exc,
                )
                failed_deletes.append(old_path)

        result.replaced_files = len(existing) - len(failed_deletes)

        if failed_deletes:
            result.target_file = final_key  # new file is live; record it for recovery
            result.status = "failed"
            result.error = (
                f"delete of old normalized file(s) failed after rename; "
                f"double-count risk — manual cleanup required: {failed_deletes}"
            )
            return result

    result.target_file = final_key
    result.status = "ok"
    LOG.info(
        "wrote unit %s %s %d-%02d: %d rows → %s (replaced=%d)",
        unit.dataset, unit.source or "-", unit.year, unit.month,
        rows_written, final_key, result.replaced_files,
    )
    return result


def _dry_run_unit(
    unit: RewriteUnit,
    fs,
    bucket: str,
    replace_existing: bool = False,
) -> UnitResult:
    """Report planned rewrite for one unit. Reads footer metadata only; never writes."""
    result = UnitResult(
        dataset=unit.dataset,
        source=unit.source,
        year=unit.year,
        month=unit.month,
        source_prefix=unit.source_prefix,
        target_prefix=unit.target_prefix,
        source_files=len(unit.source_keys),
        source_bytes=unit.source_bytes,
    )

    if not unit.source_keys:
        result.status = "skipped"
        result.error = "no source files"
        return result

    existing = _list_target_parquet(fs, bucket, unit.target_prefix)
    if existing and not replace_existing:
        result.status = "skipped"
        return result
    elif existing:
        # Will replace; record count for the report.
        result.replaced_files = len(existing)

    # Read Parquet footer metadata (no column data) for the report.
    total_rows = 0
    all_fps: set[str] = set()
    for key in unit.source_keys:
        try:
            meta = pq.read_metadata(f"s3://{bucket}/{key}", filesystem=fs)
        except Exception as exc:
            LOG.warning("metadata read failed for %s: %s", key, exc)
            continue
        schema = meta.schema.to_arrow_schema()
        all_fps.add(_schema_fingerprint(schema))
        total_rows += sum(
            meta.row_group(i).num_rows for i in range(meta.num_row_groups)
        )

    result.rows_source = total_rows
    result.schema_fingerprint = next(iter(all_fps)) if len(all_fps) == 1 else None
    result.status = "ok"   # "ok" in dry-run means: would proceed
    return result


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def build_report(
    unit_results: list[UnitResult],
    *,
    dry_run: bool,
    bucket: str,
    datasets: list[str],
) -> dict:
    units_json = [
        {
            "dataset": r.dataset,
            "source": r.source,
            "year": r.year,
            "month": r.month,
            "source_prefix": r.source_prefix,
            "target_prefix": r.target_prefix,
            "source_files": r.source_files,
            "source_bytes": r.source_bytes,
            "rows_source": r.rows_source,
            "rows_written": r.rows_written,
            "ts_min": r.ts_min,
            "ts_max": r.ts_max,
            "schema_fingerprint": r.schema_fingerprint,
            "status": r.status,
            "error": r.error,
            "baseline_mismatch": r.baseline_mismatch,
            "replaced_files": r.replaced_files,
        }
        for r in unit_results
    ]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "dry_run" if dry_run else "apply",
        "bucket": bucket,
        "datasets": datasets,
        "units": units_json,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_month(s: str) -> tuple[int, int]:
    """Parse 'YYYY-MM' → (year, month). Raises ValueError on bad format."""
    try:
        dt = datetime.strptime(s, "%Y-%m")
        return dt.year, dt.month
    except ValueError:
        raise ValueError(f"invalid --month format (expected YYYY-MM): {s!r}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Rewrite silver/ops Parquet from current layout to normalized "
            "month-level layout. Default is dry-run; pass --apply to write. "
            "Never deletes old-prefix files."
        )
    )

    sel = parser.add_argument_group("Selectors (mutually exclusive; one is required)")
    sel.add_argument(
        "--dataset",
        dest="datasets",
        action="append",
        choices=SUPPORTED_DATASETS,
        metavar="DATASET",
        help="Dataset to rewrite (repeatable). Choices: " + ", ".join(SUPPORTED_DATASETS),
    )
    sel.add_argument("--all", action="store_true", help="Rewrite all supported datasets")

    filt = parser.add_argument_group("Filters")
    filt.add_argument(
        "--source",
        choices=sorted(SILVER_SOURCES),
        help="For silver_observations only: limit to one source (carousel|detail|srp)",
    )
    filt.add_argument(
        "--month",
        metavar="YYYY-MM",
        help="Limit to one calendar month",
    )
    filt.add_argument(
        "--limit-partitions",
        type=int,
        metavar="N",
        help="Max rewrite units to process per run",
    )

    safe = parser.add_argument_group("Safety")
    safe.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="List planned rewrites without writing (default when --apply is absent)",
    )
    safe.add_argument(
        "--apply",
        action="store_true",
        help="Write to normalized prefix (required for writes; default is dry-run)",
    )
    safe.add_argument(
        "--baseline-audit",
        type=Path,
        metavar="PATH",
        help="Phase 3 audit JSON for row-count cross-check",
    )
    safe.add_argument(
        "--json-out",
        type=Path,
        metavar="PATH",
        help="Write verification report to PATH",
    )
    safe.add_argument(
        "--replace-existing-target",
        action="store_true",
        help=(
            "Replace normalized output even if the target prefix already has "
            ".parquet files (Phase 6 delta rewrite after final flush). "
            "Old normalized files are deleted after the new file is validated "
            "and renamed. Baseline check becomes a regression guard: "
            "source_rows >= baseline_rows is accepted; only source_rows < "
            "baseline_rows fails. Old-prefix files are never touched."
        ),
    )

    other = parser.add_argument_group("Other")
    other.add_argument(
        "--bucket",
        default=BUCKET,
        help=f"MinIO bucket [default: $MINIO_BUCKET or '{BUCKET}']",
    )
    other.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING"],
    )

    args = parser.parse_args(argv)

    # --dataset and --all are mutually exclusive; one is required.
    if args.all and args.datasets:
        parser.error("--all and --dataset are mutually exclusive")
    if not args.all and not args.datasets:
        parser.error("one of --dataset or --all is required")

    # --source is only meaningful for silver_observations.
    if args.source and args.datasets and "silver_observations" not in args.datasets:
        parser.error(
            "--source is only valid when silver_observations is selected via --dataset"
        )

    # Validate --month format.
    if args.month:
        try:
            _parse_month(args.month)
        except ValueError as exc:
            parser.error(str(exc))

    # --apply overrides implicit dry-run; set a single flag for clarity.
    if not args.apply:
        args.dry_run = True

    return args


def _print_summary(results: list[UnitResult], mode: str) -> None:
    ok = sum(1 for r in results if r.status == "ok")
    skipped = sum(1 for r in results if r.status == "skipped")
    failed = sum(1 for r in results if r.status == "failed")
    total_rows = sum(r.rows_source or 0 for r in results)
    print("")
    print(f"=== Parquet Layout Rewrite ({mode}) ===")
    print(f"Units:   {len(results):>6}  (ok={ok}  skipped={skipped}  failed={failed})")
    print(f"Rows:    {total_rows:>10,}")
    if failed:
        print(f"FAILURES: {failed}")
        for r in results:
            if r.status == "failed":
                print(
                    f"  {r.dataset} {r.source or '-'} "
                    f"{r.year}-{r.month:02d}: {r.error}"
                )
    print("")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    mode = "APPLY" if args.apply else "DRY-RUN"
    LOG.info("%s mode — bucket=%s", mode, args.bucket)
    if not args.apply:
        LOG.info("Pass --apply to perform actual writes.")

    month_filter: Optional[tuple[int, int]] = None
    if args.month:
        month_filter = _parse_month(args.month)

    selected_datasets = SUPPORTED_DATASETS if args.all else args.datasets

    client = get_boto3_client()
    fs = get_s3fs()

    baseline: dict[str, dict] = {}
    if args.baseline_audit:
        try:
            baseline = load_baseline_audit(args.baseline_audit)
            LOG.info("Loaded baseline audit from %s", args.baseline_audit)
        except Exception as exc:
            LOG.error("Failed to load baseline audit: %s", exc)
            return 1

    LOG.info("Discovering rewrite units...")
    units = discover_units(
        client,
        args.bucket,
        selected_datasets,
        month_filter=month_filter,
        source_filter=args.source,
        limit_partitions=args.limit_partitions,
    )
    LOG.info("Found %d unit(s) to process", len(units))

    replace_existing = args.replace_existing_target

    unit_results: list[UnitResult] = []
    for unit in units:
        baseline_rows = _baseline_lookup(baseline, unit) if baseline else None
        if args.apply:
            result = _apply_unit(
                unit, fs, args.bucket, baseline_rows,
                replace_existing=replace_existing,
            )
        else:
            result = _dry_run_unit(unit, fs, args.bucket, replace_existing=replace_existing)
        unit_results.append(result)
        LOG.info(
            "[%s] %s %s %d-%02d  files=%d bytes=%d rows=%s status=%s",
            mode,
            result.dataset,
            result.source or "-",
            result.year,
            result.month,
            result.source_files,
            result.source_bytes,
            result.rows_source,
            result.status,
        )

    report = build_report(
        unit_results,
        dry_run=not args.apply,
        bucket=args.bucket,
        datasets=selected_datasets,
    )
    _print_summary(unit_results, mode)

    if args.json_out:
        args.json_out.write_text(json.dumps(report, indent=2))
        LOG.info("Wrote report to %s", args.json_out)

    failed = sum(1 for r in unit_results if r.status == "failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
