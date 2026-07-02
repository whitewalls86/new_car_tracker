"""Read-only audit of the MinIO Parquet lake layout.

Reports object counts, bytes, partition structure, schema variants, and row counts
for silver and ops event datasets. Never writes to MinIO.

Usage:
  python scripts/audit_parquet_layout.py --all
  python scripts/audit_parquet_layout.py --dataset silver_observations
  python scripts/audit_parquet_layout.py \\
      --dataset silver_observations \\
      --dataset price_observation_events \\
      --json-out /tmp/audit.json \\
      --markdown-out /tmp/audit.md
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from shared.minio import BUCKET, get_boto3_client, get_s3fs

LOG = logging.getLogger("audit_parquet_layout")

SMALL_FILE_THRESHOLD = 1 * 1024 * 1024  # 1 MiB

SUPPORTED_DATASETS = [
    "silver_observations",
    "price_observation_events",
    "vin_to_listing_events",
    "blocked_cooldown_events",
    "detail_scrape_claim_events",
    "artifacts_queue_events",
]


def _ops_re(table: str) -> re.Pattern:
    return re.compile(
        rf"^ops/{re.escape(table)}/year=\d+/month=\d+/[^/]+\.parquet$"
    )


DATASET_CONFIGS: dict[str, dict] = {
    "silver_observations": {
        "prefix": "silver/observations/",
        "expected_pattern": re.compile(
            r"^silver/observations/source=[^/]+/obs_year=\d+/obs_month=\d+/obs_day=\d+/[^/]+\.parquet$"
        ),
        "partition_template": "source=<source>/obs_year=<Y>/obs_month=<M>/obs_day=<D>/",
    },
    "price_observation_events": {
        "prefix": "ops/price_observation_events/",
        "expected_pattern": _ops_re("price_observation_events"),
        "partition_template": "year=<Y>/month=<M>/",
    },
    "vin_to_listing_events": {
        "prefix": "ops/vin_to_listing_events/",
        "expected_pattern": _ops_re("vin_to_listing_events"),
        "partition_template": "year=<Y>/month=<M>/",
    },
    "blocked_cooldown_events": {
        "prefix": "ops/blocked_cooldown_events/",
        "expected_pattern": _ops_re("blocked_cooldown_events"),
        "partition_template": "year=<Y>/month=<M>/",
    },
    "detail_scrape_claim_events": {
        "prefix": "ops/detail_scrape_claim_events/",
        "expected_pattern": _ops_re("detail_scrape_claim_events"),
        "partition_template": "year=<Y>/month=<M>/",
    },
    "artifacts_queue_events": {
        "prefix": "ops/artifacts_queue_events/",
        "expected_pattern": _ops_re("artifacts_queue_events"),
        "partition_template": "year=<Y>/month=<M>/",
    },
}


# ── Data structures ───────────────────────────────────────────────────────────


@dataclass
class PartitionStat:
    path: str
    objects: int = 0
    bytes: int = 0
    all_keys: list[str] = field(default_factory=list)


@dataclass
class FileMetaInfo:
    rows: int
    schema_fingerprint: str
    ts_min: Optional[str]
    ts_max: Optional[str]


# ── Pure helpers ──────────────────────────────────────────────────────────────


def _schema_fingerprint(schema) -> str:
    """Return a short hash identifying a PyArrow schema by its field names and types.

    Fields are sorted by name before hashing so that column reordering between
    files does not produce spurious schema variants. The intent is logical schema
    compatibility: same columns + types = same fingerprint regardless of order.
    """
    canonical = ",".join(
        f"{f.name}:{f.type}" for f in sorted(schema, key=lambda f: f.name)
    )
    return hashlib.md5(canonical.encode()).hexdigest()[:12]  # noqa: S324


def _partition_path_of(key: str) -> str:
    """Return the directory portion of an object key (trailing slash included)."""
    return key.rsplit("/", 1)[0] + "/"


def _is_expected_key(key: str, expected_pattern: re.Pattern) -> bool:
    return bool(expected_pattern.match(key))


# ── Parquet metadata reading (read-only) ─────────────────────────────────────


def read_file_metadata(bucket: str, key: str, fs) -> Optional[FileMetaInfo]:
    """Read Parquet file footer metadata from MinIO. Returns None on failure.

    Uses pyarrow.parquet.read_metadata which reads only the footer — no column
    data is transferred.
    """
    import pyarrow.parquet as pq

    try:
        meta = pq.read_metadata(f"s3://{bucket}/{key}", filesystem=fs)
    except Exception as exc:
        LOG.warning("metadata read failed: %s — %s", key, exc)
        return None

    schema = meta.schema.to_arrow_schema()
    fp = _schema_fingerprint(schema)

    rows = sum(meta.row_group(i).num_rows for i in range(meta.num_row_groups))

    ts_min: Optional[str] = None
    ts_max: Optional[str] = None
    try:
        ts_min, ts_max = _extract_ts_range(meta)
    except Exception:
        pass  # stats not available; leave as null

    return FileMetaInfo(rows=rows, schema_fingerprint=fp, ts_min=ts_min, ts_max=ts_max)


_TS_COL_CANDIDATES = frozenset({"fetched_at", "event_at", "written_at"})


def _extract_ts_range(meta) -> tuple[Optional[str], Optional[str]]:
    """Try to extract min/max timestamp from Parquet row group statistics.

    Returns (None, None) if statistics are unavailable for any timestamp column.
    Only checks columns in _TS_COL_CANDIDATES to avoid scanning all columns.
    """
    import pyarrow as pa

    schema = meta.schema.to_arrow_schema()
    ts_fields = [
        f.name for f in schema
        if f.name in _TS_COL_CANDIDATES and pa.types.is_timestamp(f.type)
    ]
    if not ts_fields:
        return None, None

    col_name = ts_fields[0]
    col_idx = schema.get_field_index(col_name)

    overall_min: Optional[int] = None
    overall_max: Optional[int] = None

    for rg_idx in range(meta.num_row_groups):
        rg = meta.row_group(rg_idx)
        col = rg.column(col_idx)
        stats = col.statistics
        if stats is None or not stats.has_min_max:
            return None, None  # bail if any row group lacks stats
        # Statistics values are native Python objects for timestamp columns
        mn = stats.min
        mx = stats.max
        if mn is None or mx is None:
            return None, None
        # Convert to microseconds epoch if they are datetime objects
        if isinstance(mn, datetime):
            mn = int(mn.timestamp() * 1_000_000)
        if isinstance(mx, datetime):
            mx = int(mx.timestamp() * 1_000_000)
        overall_min = mn if overall_min is None else min(overall_min, mn)
        overall_max = mx if overall_max is None else max(overall_max, mx)

    if overall_min is None or overall_max is None:
        return None, None

    def _to_iso(us: int) -> str:
        return datetime.fromtimestamp(us / 1_000_000, tz=timezone.utc).isoformat()

    return _to_iso(overall_min), _to_iso(overall_max)


# ── Listing ───────────────────────────────────────────────────────────────────


def iter_objects(client, bucket: str, prefix: str):
    """Yield (key, size) for every object under prefix (streaming pagination)."""
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for entry in page.get("Contents", []):
            yield entry["Key"], entry["Size"]


# ── Core audit ────────────────────────────────────────────────────────────────


def audit_dataset(
    client,
    fs,
    bucket: str,
    dataset_name: str,
    config: dict,
    sample_files: int = 3,
) -> dict:
    """Audit one dataset. Returns a dict matching the JSON report schema."""
    prefix = config["prefix"]
    expected_re: re.Pattern = config["expected_pattern"]

    partitions: dict[str, PartitionStat] = {}
    total_objects = 0
    total_bytes = 0
    small_files = 0
    unexpected_paths: list[str] = []

    LOG.info("auditing %s (prefix=%s)", dataset_name, prefix)

    for key, size in iter_objects(client, bucket, prefix):
        total_objects += 1
        total_bytes += size

        if size < SMALL_FILE_THRESHOLD:
            small_files += 1

        if not _is_expected_key(key, expected_re):
            unexpected_paths.append(key)
            LOG.debug("unexpected path: %s", key)
            continue

        part_path = _partition_path_of(key)
        if part_path not in partitions:
            partitions[part_path] = PartitionStat(path=part_path)
        p = partitions[part_path]
        p.objects += 1
        p.bytes += size
        p.all_keys.append(key)

    LOG.info(
        "%s: %d objects, %d bytes, %d partitions, %d small, %d unexpected",
        dataset_name, total_objects, total_bytes,
        len(partitions), small_files, len(unexpected_paths),
    )

    # Read Parquet footer metadata per partition.
    #
    # Row counts and timestamps: read ALL files in each partition so the JSON
    # report is safe as the Phase 5 rewrite baseline.
    #
    # Schema sampling: collect fingerprints only for the first --sample-files
    # files per partition to bound the number of footer reads for large
    # uncompacted partitions.
    all_schema_fingerprints: set[str] = set()
    partition_list: list[dict] = []

    for p in partitions.values():
        partition_rows = 0
        meta_sampled = 0
        meta_read = 0
        meta_failures = 0
        partition_schema_fps: set[str] = set()
        schema_samples_collected = 0
        ts_min: Optional[str] = None
        ts_max: Optional[str] = None

        for key in p.all_keys:
            meta_sampled += 1
            info = read_file_metadata(bucket, key, fs)
            if info is None:
                meta_failures += 1
                continue
            meta_read += 1
            partition_rows += info.rows

            if info.ts_min is not None:
                ts_min = info.ts_min if ts_min is None else min(ts_min, info.ts_min)
            if info.ts_max is not None:
                ts_max = info.ts_max if ts_max is None else max(ts_max, info.ts_max)

            if schema_samples_collected < sample_files:
                partition_schema_fps.add(info.schema_fingerprint)
                all_schema_fingerprints.add(info.schema_fingerprint)
                schema_samples_collected += 1

        schema_fp = (
            next(iter(partition_schema_fps))
            if len(partition_schema_fps) == 1
            else None
        )
        # rows is None when every footer read failed — distinguishes a bad S3
        # path or filesystem issue from a genuinely empty partition.
        rows: Optional[int] = partition_rows if meta_read > 0 else None

        partition_list.append({
            "path": p.path,
            "objects": p.objects,
            "bytes": p.bytes,
            "rows": rows,
            "metadata_sampled": meta_sampled,
            "metadata_read": meta_read,
            "metadata_failures": meta_failures,
            "schema_fingerprint": schema_fp,
            "ts_min": ts_min,
            "ts_max": ts_max,
        })

    # Sort partitions for deterministic output
    partition_list.sort(key=lambda x: x["path"])

    return {
        "prefix": prefix,
        "expected_partition_pattern": config["partition_template"],
        "total_objects": total_objects,
        "total_bytes": total_bytes,
        "partition_count": len(partitions),
        "small_files": small_files,
        "schema_variants": len(all_schema_fingerprints),
        "unexpected_paths": unexpected_paths,
        "partitions": partition_list,
    }


# ── Report builders ───────────────────────────────────────────────────────────


def build_json_report(dataset_results: dict[str, dict]) -> dict:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "datasets": dataset_results,
    }


def _fmt_bytes(n: int) -> str:
    if n >= 1024 ** 3:
        return f"{n / 1024 ** 3:.2f} GiB"
    if n >= 1024 ** 2:
        return f"{n / 1024 ** 2:.1f} MiB"
    if n >= 1024:
        return f"{n / 1024:.1f} KiB"
    return f"{n} B"


def build_markdown_report(report: dict) -> str:
    lines: list[str] = [
        "# Parquet Lake Layout Audit",
        f"Generated: {report.get('generated_at', 'unknown')}",
        "",
        "## Summary",
        "",
        "| Dataset | Objects | Total Size | Partitions | Small Files"
        " | Schema Variants | Unexpected Paths |",
        "|---------|---------|------------|------------|-------------|-----------------|-----------------|",
    ]

    for name, ds in report.get("datasets", {}).items():
        lines.append(
            f"| {name} "
            f"| {ds['total_objects']:,} "
            f"| {_fmt_bytes(ds['total_bytes'])} "
            f"| {ds['partition_count']:,} "
            f"| {ds['small_files']:,} "
            f"| {ds['schema_variants']} "
            f"| {len(ds['unexpected_paths'])} |"
        )

    lines += ["", "## Dataset Details", ""]

    for name, ds in report.get("datasets", {}).items():
        lines += [
            f"### {name}",
            "",
            f"- **Prefix:** `{ds['prefix']}`",
            f"- **Expected partition pattern:** `{ds['expected_partition_pattern']}`",
            f"- **Total objects:** {ds['total_objects']:,}",
            f"- **Total size:** {_fmt_bytes(ds['total_bytes'])}",
            f"- **Partition count:** {ds['partition_count']:,}",
            f"- **Small files (< 1 MiB):** {ds['small_files']:,}",
            f"- **Schema variants:** {ds['schema_variants']}",
            "",
        ]

        if ds["unexpected_paths"]:
            lines.append(f"**Unexpected paths ({len(ds['unexpected_paths'])}):**")
            for p in ds["unexpected_paths"][:20]:
                lines.append(f"- `{p}`")
            if len(ds["unexpected_paths"]) > 20:
                lines.append(f"- … {len(ds['unexpected_paths']) - 20} more")
            lines.append("")

        if ds["partitions"]:
            lines += [
                "| Partition | Objects | Bytes | Rows | Meta OK/Total | Schema | TS Min | TS Max |",
                "|-----------|---------|-------|------|---------------|--------|--------|--------|",
            ]
            for part in ds["partitions"]:
                rows_str = str(part["rows"]) if part["rows"] is not None else "—"
                meta_str = (
                    f"{part['metadata_read']}/{part['metadata_sampled']}"
                )
                fp_str = part["schema_fingerprint"] or "mixed"
                ts_min = part["ts_min"] or "—"
                ts_max = part["ts_max"] or "—"
                lines.append(
                    f"| `{part['path']}` "
                    f"| {part['objects']} "
                    f"| {_fmt_bytes(part['bytes'])} "
                    f"| {rows_str} "
                    f"| {meta_str} "
                    f"| {fp_str} "
                    f"| {ts_min} "
                    f"| {ts_max} |"
                )
            lines.append("")
        else:
            lines += ["*(no expected partitions found)*", ""]

    return "\n".join(lines)


def print_stdout_summary(report: dict) -> None:
    print("")
    print("=== Parquet Lake Layout Audit ===")
    print(f"Generated: {report.get('generated_at', 'unknown')}")
    print("")
    for name, ds in report.get("datasets", {}).items():
        print(f"  {name}")
        print(f"    objects:      {ds['total_objects']:>10,}")
        print(f"    size:         {_fmt_bytes(ds['total_bytes']):>12}")
        print(f"    partitions:   {ds['partition_count']:>10,}")
        print(f"    small files:  {ds['small_files']:>10,}")
        print(f"    schema vars:  {ds['schema_variants']:>10}")
        print(f"    unexpected:   {len(ds['unexpected_paths']):>10}")
        print("")


# ── CLI ───────────────────────────────────────────────────────────────────────


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only audit of MinIO Parquet lake layout. "
            "Never writes to MinIO. "
            "Use --all or --dataset (repeatable) to select datasets."
        )
    )

    sel = parser.add_argument_group("Selectors (mutually exclusive; one is required)")
    sel.add_argument(
        "--dataset",
        dest="datasets",
        action="append",
        choices=SUPPORTED_DATASETS,
        metavar="DATASET",
        help=(
            "Dataset to audit (repeatable). "
            "Choices: " + ", ".join(SUPPORTED_DATASETS)
        ),
    )
    sel.add_argument(
        "--all",
        action="store_true",
        help="Audit all supported datasets",
    )

    out = parser.add_argument_group("Output")
    out.add_argument("--json-out", type=Path, help="Write JSON report to PATH")
    out.add_argument("--markdown-out", type=Path, help="Write Markdown report to PATH")

    other = parser.add_argument_group("Other")
    other.add_argument(
        "--bucket",
        default=BUCKET,
        help=f"MinIO bucket [default: $MINIO_BUCKET or '{BUCKET}']",
    )
    other.add_argument(
        "--sample-files",
        type=int,
        default=3,
        metavar="N",
        help="Max Parquet files to sample per partition for schema/metadata [default: 3]",
    )
    other.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING"],
        help="Logging level [default: INFO]",
    )

    args = parser.parse_args(argv)

    if args.all and args.datasets:
        parser.error("--all and --dataset are mutually exclusive")
    if not args.all and not args.datasets:
        parser.error("one of --dataset or --all is required")

    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    selected = SUPPORTED_DATASETS if args.all else args.datasets

    client = get_boto3_client()
    fs = get_s3fs()

    dataset_results: dict[str, dict] = {}
    for name in selected:
        config = DATASET_CONFIGS[name]
        result = audit_dataset(
            client, fs, args.bucket, name, config, sample_files=args.sample_files
        )
        dataset_results[name] = result

    report = build_json_report(dataset_results)
    print_stdout_summary(report)

    if args.json_out:
        args.json_out.write_text(json.dumps(report, indent=2))
        LOG.info("Wrote JSON report to %s", args.json_out)

    if args.markdown_out:
        args.markdown_out.write_text(build_markdown_report(report))
        LOG.info("Wrote Markdown report to %s", args.markdown_out)

    return 0


if __name__ == "__main__":
    sys.exit(main())
