"""Recompress existing bronze HTML .html.zst objects to zstd level 9.

Default mode is dry-run: no writes to MinIO. Pass --apply to write.
Never deletes objects.

Usage examples:
  python scripts/recompress_bronze_html.py \\
      --year 2026 --month 6 --artifact-type detail_page \\
      --limit 1000 --progress-every 100

  python scripts/recompress_bronze_html.py \\
      --prefix html/year=2026/month=6/artifact_type=detail_page/ \\
      --apply --checkpoint /tmp/recompress_2026_06.json \\
      --progress-every 500 --json-out /tmp/result_2026_06.json
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

from shared.minio import BUCKET, get_boto3_client, get_s3fs

LOG = logging.getLogger("recompress_bronze_html")
_TARGET_LEVEL = 9


# ── Data classes ──────────────────────────────────────────────────────────────


@dataclass
class ObjectInfo:
    key: str
    size: int  # compressed bytes from listing metadata


@dataclass
class Summary:
    scanned: int = 0
    processed: int = 0
    recompressed: int = 0
    skipped: int = 0
    failed: int = 0
    old_bytes: int = 0
    new_bytes: int = 0

    @property
    def saved_bytes(self) -> int:
        return self.old_bytes - self.new_bytes

    @property
    def savings_pct(self) -> float:
        return 100.0 * self.saved_bytes / self.old_bytes if self.old_bytes > 0 else 0.0

    def to_dict(self) -> dict:
        return {
            "scanned": self.scanned,
            "processed": self.processed,
            "recompressed": self.recompressed,
            "skipped": self.skipped,
            "failed": self.failed,
            "old_bytes": self.old_bytes,
            "new_bytes": self.new_bytes,
            "saved_bytes": self.saved_bytes,
            "savings_pct": round(self.savings_pct, 4),
        }


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

    month_pairs = _discover_months_for_year(fs, args.bucket, year, artifact_type)
    return [
        f"html/year={y}/month={m}/artifact_type={artifact_type}/"
        for y, m in month_pairs
    ]


def _discover_months_for_year(
    fs, bucket: str, year: int, artifact_type: str
) -> list[tuple[int, int]]:
    year_path = f"{bucket}/html/year={year}"
    months: list[tuple[int, int]] = []
    try:
        for entry in fs.ls(year_path, detail=True):
            name = str(entry.get("name", ""))
            if "/month=" not in name:
                continue
            try:
                month = int(name.rsplit("month=", 1)[1].split("/", 1)[0])
            except ValueError:
                continue
            if fs.exists(
                f"{bucket}/html/year={year}/month={month}/artifact_type={artifact_type}/"
            ):
                months.append((year, month))
    except FileNotFoundError:
        pass
    return sorted(months)


def iter_prefix(client, bucket: str, prefix: str):
    """Yield ObjectInfo for every .html.zst file under prefix (streaming pagination)."""
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for entry in page.get("Contents", []):
            key = entry["Key"]
            if key.endswith(".html.zst"):
                yield ObjectInfo(key=key, size=entry["Size"])


# ── Checkpoint ────────────────────────────────────────────────────────────────


def load_checkpoint(path: Path) -> set[str]:
    """Return set of already-processed keys from a checkpoint file, or empty set."""
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text())
        return set(data.get("processed_keys", []))
    except Exception as exc:
        LOG.warning("Failed to load checkpoint %s: %s — starting fresh", path, exc)
        return set()


def save_checkpoint(path: Path, processed_keys: set[str], summary: Summary) -> None:
    """Write checkpoint JSON atomically (write-to-tmp + rename)."""
    tmp = path.with_suffix(".tmp")
    tmp.write_text(
        json.dumps(
            {"processed_keys": sorted(processed_keys), "summary": summary.to_dict()},
            indent=2,
        )
    )
    tmp.replace(path)


# ── Core processing ───────────────────────────────────────────────────────────


def process_object(
    client,
    bucket: str,
    obj: ObjectInfo,
    *,
    apply: bool,
    force: bool,
    checkpoint_keys: set[str],
    checkpoint_path: Path | None,
    summary: Summary,
) -> None:
    """Download, recompress, and conditionally write one object. Mutates summary in-place."""
    import zstandard as zstd
    from zstandard import ZstdError

    try:
        old_compressed = client.get_object(Bucket=bucket, Key=obj.key)["Body"].read()
    except Exception as exc:
        LOG.warning("download failed: %s — %s", obj.key, exc)
        summary.failed += 1
        return

    try:
        raw = zstd.ZstdDecompressor().decompress(old_compressed)
    except (ZstdError, Exception) as exc:
        LOG.warning("decompress failed: %s — %s", obj.key, exc)
        summary.failed += 1
        return

    try:
        new_compressed = zstd.ZstdCompressor(level=_TARGET_LEVEL).compress(raw)
    except Exception as exc:
        LOG.warning("recompress failed: %s — %s", obj.key, exc)
        summary.failed += 1
        return

    old_size = len(old_compressed)
    new_size = len(new_compressed)
    worth_writing = new_size < old_size or force

    if not worth_writing:
        summary.processed += 1
        summary.skipped += 1
        # Track bytes for skipped objects (unchanged: before == after).
        # This keeps savings_pct meaningful across all processed objects, not
        # just the subset that was rewritten.
        summary.old_bytes += old_size
        summary.new_bytes += old_size
        return

    if not apply:
        # Dry-run: record what would be written
        summary.processed += 1
        summary.recompressed += 1
        summary.old_bytes += old_size
        summary.new_bytes += new_size
        return

    try:
        client.put_object(
            Bucket=bucket,
            Key=obj.key,
            Body=new_compressed,
            ContentEncoding="zstd",
            ContentType="text/html",
        )
    except Exception as exc:
        LOG.warning("put_object failed: %s — %s", obj.key, exc)
        summary.failed += 1
        return

    summary.processed += 1
    summary.recompressed += 1
    summary.old_bytes += old_size
    summary.new_bytes += new_size

    checkpoint_keys.add(obj.key)
    if checkpoint_path:
        save_checkpoint(checkpoint_path, checkpoint_keys, summary)

    LOG.debug(
        "recompressed: %s old=%d new=%d saved=%d",
        obj.key, old_size, new_size, old_size - new_size,
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


def log_progress(summary: Summary, current_key: str, progress_every: int) -> None:
    if not progress_every or summary.scanned % progress_every != 0:
        return
    LOG.info(
        "PROGRESS | scanned=%d processed=%d recompressed=%d skipped=%d failed=%d "
        "saved=%s (%.1f%%) | %s",
        summary.scanned,
        summary.processed,
        summary.recompressed,
        summary.skipped,
        summary.failed,
        _fmt_bytes(summary.saved_bytes),
        summary.savings_pct,
        current_key,
    )


def print_summary(
    summary: Summary, *, dry_run: bool, json_out: Path | None = None
) -> None:
    mode = "DRY-RUN" if dry_run else "APPLY"
    lines = [
        "",
        f"=== Bronze HTML Recompression Summary ({mode}) ===",
        f"Scanned:      {summary.scanned:>10,}",
        f"Processed:    {summary.processed:>10,}",
        f"Recompressed: {summary.recompressed:>10,}",
        f"Skipped:      {summary.skipped:>10,}",
        f"Failed:       {summary.failed:>10,}",
        "",
        f"Old bytes:    {_fmt_bytes(summary.old_bytes):>12}",
        f"New bytes:    {_fmt_bytes(summary.new_bytes):>12}",
        f"Saved bytes:  {_fmt_bytes(summary.saved_bytes):>12}",
        f"Savings:      {summary.savings_pct:>10.1f}%",
    ]
    print("\n".join(lines))

    if json_out:
        data = summary.to_dict()
        data["mode"] = mode.lower().replace("-", "_")
        Path(json_out).write_text(json.dumps(data, indent=2))
        LOG.info("Wrote JSON summary to %s", json_out)


# ── CLI ───────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Recompress existing bronze HTML .html.zst objects to zstd level 9. "
            "Default mode is dry-run. Pass --apply to write. "
            "Never deletes objects."
        )
    )

    sel = parser.add_argument_group(
        "Selector (--prefix overrides --year/--month/--artifact-type)"
    )
    sel.add_argument(
        "--prefix",
        help="Exact MinIO prefix, e.g. html/year=2026/month=6/artifact_type=detail_page/",
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
    perf.add_argument(
        "--limit", type=int, default=0,
        help="Stop after N objects scanned [0=no limit]",
    )
    perf.add_argument(
        "--max-bytes", type=int, default=0,
        help="Stop after N compressed bytes scanned from listing [0=no limit]",
    )
    perf.add_argument(
        "--progress-every", type=int, default=500,
        help="Log a progress line every N objects scanned [default: 500]",
    )
    perf.add_argument(
        "--checkpoint", type=Path,
        help="JSON checkpoint file; load processed keys on start, append on each apply",
    )
    perf.add_argument("--json-out", type=Path, help="Write final summary JSON to PATH")

    apply_grp = parser.add_argument_group("Apply mode (default is dry-run)")
    apply_grp.add_argument(
        "--apply", action="store_true",
        help="Write recompressed objects to MinIO",
    )
    apply_grp.add_argument(
        "--force", action="store_true",
        help="In apply mode, overwrite even if recompressed output is larger",
    )

    other = parser.add_argument_group("Other")
    other.add_argument(
        "--bucket", default=BUCKET,
        help=f"MinIO bucket [default: $MINIO_BUCKET or '{BUCKET}']",
    )
    other.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING"],
        help="Logging level [default: INFO]",
    )

    args = parser.parse_args()

    if args.month is not None and args.year is None:
        parser.error("--month requires --year")
    if args.prefix is None and args.year is None:
        parser.error("one of --prefix or --year is required")
    if args.force and not args.apply:
        parser.error("--force requires --apply")

    return args


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if not args.apply:
        LOG.info("DRY-RUN mode — no writes to MinIO. Pass --apply to write.")

    fs = get_s3fs()
    client = get_boto3_client()
    summary = Summary()

    checkpoint_keys: set[str] = set()
    if args.checkpoint:
        checkpoint_keys = load_checkpoint(args.checkpoint)
        if checkpoint_keys:
            LOG.info("Loaded checkpoint: %d already-processed keys", len(checkpoint_keys))

    prefixes = build_prefixes(args, fs)
    if not prefixes:
        LOG.error("No prefixes found for the given selector.")
        return 1

    LOG.info(
        "Bucket: %s | Prefixes: %s | apply=%s force=%s limit=%d max_bytes=%d",
        args.bucket, prefixes, args.apply, args.force, args.limit, args.max_bytes,
    )

    scanned_bytes = 0
    done = False

    for prefix in prefixes:
        if done:
            break
        for obj in iter_prefix(client, args.bucket, prefix):
            summary.scanned += 1

            if obj.key in checkpoint_keys:
                LOG.debug("checkpoint skip: %s", obj.key)
                continue

            process_object(
                client,
                args.bucket,
                obj,
                apply=args.apply,
                force=args.force,
                checkpoint_keys=checkpoint_keys,
                checkpoint_path=args.checkpoint,
                summary=summary,
            )

            scanned_bytes += obj.size
            log_progress(summary, obj.key, args.progress_every)

            if args.limit and summary.scanned >= args.limit:
                LOG.info("Stopping at --limit=%d", args.limit)
                done = True
                break
            if args.max_bytes and scanned_bytes >= args.max_bytes:
                LOG.info("Stopping at --max-bytes=%d", args.max_bytes)
                done = True
                break

    print_summary(summary, dry_run=not args.apply, json_out=args.json_out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
