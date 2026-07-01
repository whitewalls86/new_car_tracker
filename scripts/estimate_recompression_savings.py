"""Estimate storage savings from recompressing bronze HTML zstd level-3 to level-9.

Read-only: never calls put_object, delete_object, copy_object, or any write API.

Usage examples:
  python scripts/estimate_recompression_savings.py \\
      --year 2026 --month 6 --sample-rate 0.05 --max-bytes 104857600

  python scripts/estimate_recompression_savings.py \\
      --prefix html/year=2026/month=5/artifact_type=results_page/ \\
      --sample-rate 0.01 --json-out /tmp/savings_results.json

  python scripts/estimate_recompression_savings.py \\
      --year 2025 --limit 50 --sample-rate 1.0
"""
from __future__ import annotations

import argparse
import json
import logging
import random
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterator

from shared.minio import BUCKET, get_boto3_client, get_s3fs

LOG = logging.getLogger("recompression_estimator")


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class ObjectInfo:
    key: str   # bare key, no bucket prefix
    size: int  # compressed bytes from listing metadata


@dataclass
class MeasurementResult:
    key: str
    old_compressed: int
    raw_bytes: int
    new_compressed: int
    saved_bytes: int
    error: str | None


@dataclass
class Stats:
    scanned: int = 0
    sampled: int = 0
    skipped: int = 0
    failed: int = 0
    listed_bytes: int = 0
    old_compressed_bytes: int = 0
    raw_bytes_total: int = 0
    new_compressed_bytes: int = 0
    failed_keys: list[str] = field(default_factory=list)


# ── Listing ───────────────────────────────────────────────────────────────────

def build_prefixes(args: argparse.Namespace, fs) -> list[str]:
    """Resolve CLI selectors to one or more MinIO prefixes (bare, no bucket prefix)."""
    if args.prefix:
        return [args.prefix]

    year = args.year
    month = args.month
    artifact_type = args.artifact_type

    if month is not None:
        return [f"html/year={year}/month={month}/artifact_type={artifact_type}/"]

    # Walk all months for the year
    month_pairs = discover_months_for_year(fs, args.bucket, year, artifact_type)
    return [
        f"html/year={y}/month={m}/artifact_type={artifact_type}/"
        for y, m in month_pairs
    ]


def discover_months_for_year(
    fs, bucket: str, year: int, artifact_type: str
) -> list[tuple[int, int]]:
    """List month integers present under html/year=Y/ for the given artifact_type."""
    year_path = f"{bucket}/html/year={year}"
    months: list[tuple[int, int]] = []
    try:
        for month_entry in fs.ls(year_path, detail=True):
            month_name = str(month_entry.get("name", ""))
            if "/month=" not in month_name:
                continue
            month_str = month_name.rsplit("month=", 1)[1].split("/", 1)[0]
            try:
                month = int(month_str)
            except ValueError:
                continue
            artifact_path = (
                f"{bucket}/html/year={year}/month={month}"
                f"/artifact_type={artifact_type}/"
            )
            if fs.exists(artifact_path):
                months.append((year, month))
    except FileNotFoundError:
        pass
    return sorted(months)


def iter_prefix(fs, bucket: str, prefix: str) -> Iterator[ObjectInfo]:
    """Yield ObjectInfo for every .html.zst file under prefix."""
    full_prefix = f"{bucket}/{prefix}" if not prefix.startswith(bucket + "/") else prefix
    try:
        entries = fs.ls(full_prefix, detail=True)
    except FileNotFoundError:
        return
    for entry in entries:
        if entry.get("type") == "directory":
            continue
        path = str(entry.get("name", ""))
        if not path.endswith(".html.zst"):
            continue
        size = int(entry.get("size") or entry.get("Size") or 0)
        bare_key = path[len(bucket) + 1:] if path.startswith(bucket + "/") else path
        yield ObjectInfo(key=bare_key, size=size)


# ── Sampling ─────────────────────────────────────────────────────────────────

def make_sampler(sample_rate: float, random_sample: bool) -> Callable[[int], bool]:
    """Return sampler(scan_index) -> bool.

    Systematic (default): stride = round(1/sample_rate), sample when scan_index % stride == 0.
    Bernoulli (--random-sample): sample when random.random() < sample_rate.
    """
    if random_sample:
        def bernoulli(scan_index: int) -> bool:
            return random.random() < sample_rate
        return bernoulli

    stride = max(1, round(1 / sample_rate)) if sample_rate > 0 else 0

    def systematic(scan_index: int) -> bool:
        if stride == 0:
            return False
        return scan_index % stride == 0

    return systematic


# ── Measurement ──────────────────────────────────────────────────────────────

def measure_object(client, bucket: str, obj: ObjectInfo) -> MeasurementResult:
    """Download, decompress, recompress at level 9, return sizes. Never writes to MinIO."""
    import zstandard as zstd
    from zstandard import ZstdError

    try:
        response = client.get_object(Bucket=bucket, Key=obj.key)
        compressed = response["Body"].read()
    except Exception as exc:
        return MeasurementResult(
            key=obj.key, old_compressed=0, raw_bytes=0,
            new_compressed=0, saved_bytes=0, error=str(exc),
        )

    try:
        raw = zstd.ZstdDecompressor().decompress(compressed)
    except (ZstdError, Exception) as exc:
        return MeasurementResult(
            key=obj.key, old_compressed=len(compressed), raw_bytes=0,
            new_compressed=0, saved_bytes=0, error=str(exc),
        )

    try:
        recompressed = zstd.ZstdCompressor(level=9).compress(raw)
    except Exception as exc:
        return MeasurementResult(
            key=obj.key, old_compressed=len(compressed), raw_bytes=len(raw),
            new_compressed=0, saved_bytes=0, error=str(exc),
        )

    return MeasurementResult(
        key=obj.key,
        old_compressed=len(compressed),
        raw_bytes=len(raw),
        new_compressed=len(recompressed),
        saved_bytes=len(compressed) - len(recompressed),
        error=None,
    )


# ── Progress + output ─────────────────────────────────────────────────────────

def _fmt_bytes(n: int) -> str:
    if n >= 1024 ** 3:
        return f"{n / 1024 ** 3:.2f} GiB"
    if n >= 1024 ** 2:
        return f"{n / 1024 ** 2:.1f} MiB"
    if n >= 1024:
        return f"{n / 1024:.1f} KiB"
    return f"{n} B"


def log_progress(stats: Stats, current_key: str, log_every: int) -> None:
    if not log_every or stats.scanned % log_every != 0:
        return
    old = stats.old_compressed_bytes
    saved = old - stats.new_compressed_bytes
    savings_pct = 100.0 * saved / old if old > 0 else 0.0
    LOG.info(
        "PROGRESS | scanned=%d sampled=%d bytes_read=%s savings=%.1f%% failures=%d | %s",
        stats.scanned, stats.sampled,
        _fmt_bytes(stats.old_compressed_bytes),
        savings_pct,
        stats.failed,
        current_key,
    )


def recommendation(savings_pct: float) -> str:
    if savings_pct >= 15.0:
        return "WORTH IT"
    if savings_pct >= 5.0:
        return "MAYBE"
    return "SKIP"


def print_summary(stats: Stats, sample_rate: float, json_out: Path | None = None) -> None:
    old = stats.old_compressed_bytes
    new = stats.new_compressed_bytes
    saved = old - new
    savings_pct = 100.0 * saved / old if old > 0 else 0.0
    listed = stats.listed_bytes
    proj_saved = int(listed * saved / old) if old > 0 else 0
    rec = recommendation(savings_pct)

    rec_detail = {
        "WORTH IT": (
            f"Adding a manual recompression pass (or shipping Track A level-9 bump now) is\n"
            f"justified. Existing objects account for ~{_fmt_bytes(proj_saved)}"
            " of recoverable storage.\n"
            f"Consider a one-off recompression batch after bumping ZSTD_LEVEL for new writes."
        ),
        "MAYBE": (
            "Savings are moderate. Worth it if storage pressure is real; otherwise\n"
            "consider Plan 114 section decomp or refresh reduction instead."
        ),
        "SKIP": (
            "Savings are too low to justify a recompression pass.\n"
            "Focus on Plan 114 section decomp or refresh reduction instead."
        ),
    }[rec]

    lines = [
        "",
        "=== Recompression Savings Estimate ===",
        f"Scanned objects:        {stats.scanned:>10,}",
        f"Sampled objects:        {stats.sampled:>10,}  ({100.0 * sample_rate:.1f}% sample rate)",
        f"Skipped objects:        {stats.skipped:>10,}",
        f"Failed objects:         {stats.failed:>10,}",
    ]
    if stats.failed_keys:
        lines.append(f"  failed keys: {stats.failed_keys}")
    lines += [
        "",
        f"Sampled bytes (old, level-3):    {_fmt_bytes(old):>12}",
        f"Decompressed raw bytes:          {_fmt_bytes(stats.raw_bytes_total):>12}",
        f"Estimated bytes (new, level-9):  {_fmt_bytes(new):>12}",
        f"Estimated saved bytes:           {_fmt_bytes(saved):>12}",
        f"Estimated savings:               {savings_pct:>10.1f}%",
        "",
        "--- Extrapolated to full scanned prefix ---",
        f"Listed prefix size:    {_fmt_bytes(listed):>12}",
        f"Projected savings:     {_fmt_bytes(proj_saved):>12}  (~{savings_pct:.1f}%)",
        "",
        "=== Recommendation ===",
        f"Savings {savings_pct:.1f}% -> {rec}",
        rec_detail,
    ]
    print("\n".join(lines))

    if json_out:
        summary = {
            "scanned": stats.scanned,
            "sampled": stats.sampled,
            "skipped": stats.skipped,
            "failed": stats.failed,
            "failed_keys": stats.failed_keys,
            "listed_bytes": listed,
            "old_compressed_bytes": old,
            "raw_bytes_total": stats.raw_bytes_total,
            "new_compressed_bytes": new,
            "saved_bytes": saved,
            "savings_pct": round(savings_pct, 4),
            "projected_saved_bytes": proj_saved,
            "recommendation": rec,
        }
        Path(json_out).write_text(json.dumps(summary, indent=2))
        LOG.info("Wrote JSON summary to %s", json_out)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Estimate MinIO bronze HTML storage savings from recompressing "
            "zstd level-3 to level-9. Read-only: never writes to MinIO."
        )
    )
    sel = parser.add_argument_group("Selector (--prefix overrides --year/--month/--artifact-type)")
    sel.add_argument(
        "--prefix",
        help="Exact MinIO prefix, e.g. html/year=2026/month=5/artifact_type=detail_page/"
             " (overrides --year/--month/--artifact-type)",
    )
    sel.add_argument("--year", type=int, help="Calendar year, e.g. 2026")
    sel.add_argument("--month", type=int, help="Calendar month integer (requires --year)")
    sel.add_argument(
        "--artifact-type",
        default="detail_page",
        choices=["detail_page", "results_page"],
        help="Artifact type [default: detail_page]",
    )

    perf = parser.add_argument_group("Safety / performance")
    perf.add_argument("--limit", type=int, default=0, help="Stop after N objects [0=no limit]")
    perf.add_argument(
        "--sample-rate", type=float, default=0.05,
        help="Fraction (0,1] of listed objects to download and measure [default: 0.05]",
    )
    perf.add_argument(
        "--max-bytes", type=int, default=0,
        help="Stop after downloading N compressed bytes [0=no limit]",
    )
    perf.add_argument(
        "--progress-every", type=int, default=500,
        help="Log a progress line every N objects scanned [default: 500]",
    )
    perf.add_argument(
        "--random-sample", action="store_true",
        help="Bernoulli sampling (random.random() < rate) instead of systematic every-Nth",
    )
    perf.add_argument("--json-out", type=Path, help="Write final summary JSON to this path")

    other = parser.add_argument_group("Other")
    other.add_argument("--bucket", default=BUCKET, help=f"MinIO bucket [default: {BUCKET}]")
    other.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING"],
        help="Logging level [default: INFO]",
    )

    args = parser.parse_args()

    if args.month is not None and args.year is None:
        parser.error("--month requires --year")
    if args.prefix is None and args.year is None:
        parser.error("one of --prefix or --year is required")
    if not (0.0 < args.sample_rate <= 1.0):
        parser.error("--sample-rate must be in (0.0, 1.0]")

    return args


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fs = get_s3fs()
    client = get_boto3_client()
    sampler = make_sampler(args.sample_rate, args.random_sample)
    stats = Stats()

    prefixes = build_prefixes(args, fs)
    if not prefixes:
        LOG.error("No prefixes found for the given selector.")
        return 1

    LOG.info(
        "Bucket: %s | Prefixes: %s | sample_rate=%.3f limit=%d max_bytes=%d",
        args.bucket, prefixes, args.sample_rate, args.limit, args.max_bytes,
    )

    done = False
    for prefix in prefixes:
        if done:
            break
        prefix_listed_bytes = 0
        prefix_listed_count = 0

        for obj in iter_prefix(fs, args.bucket, prefix):
            stats.listed_bytes += obj.size
            prefix_listed_bytes += obj.size
            prefix_listed_count += 1
            stats.scanned += 1

            if sampler(stats.scanned - 1):
                result = measure_object(client, args.bucket, obj)
                stats.sampled += 1
                if result.error:
                    stats.failed += 1
                    if len(stats.failed_keys) < 5:
                        stats.failed_keys.append(obj.key)
                    LOG.warning("measure failed: %s — %s", obj.key, result.error)
                else:
                    stats.old_compressed_bytes += result.old_compressed
                    stats.raw_bytes_total += result.raw_bytes
                    stats.new_compressed_bytes += result.new_compressed
            else:
                stats.skipped += 1

            log_progress(stats, obj.key, args.progress_every)

            if args.limit and stats.scanned >= args.limit:
                LOG.info("Stopping at --limit=%d", args.limit)
                done = True
                break
            if args.max_bytes and stats.old_compressed_bytes >= args.max_bytes:
                LOG.info("Stopping at --max-bytes=%d", args.max_bytes)
                done = True
                break

        LOG.info(
            "PREFIX DONE | prefix=%s listed=%d bytes=%s",
            prefix, prefix_listed_count, _fmt_bytes(prefix_listed_bytes),
        )

    if stats.sampled > 0 and stats.failed / stats.sampled > 0.1:
        LOG.warning(
            "High failure rate (%.1f%%) — check for corrupt objects or credential issues.",
            100.0 * stats.failed / stats.sampled,
        )

    print_summary(stats, args.sample_rate, args.json_out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
