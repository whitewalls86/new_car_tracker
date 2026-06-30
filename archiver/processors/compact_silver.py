"""
Compact silver/observations partitions: merge per-flush part files into a single sorted
compacted-<YYYY-MM-DD>.parquet per day partition.

Only processes obs_day <= today_utc - 2 (2-day watermark against late-arriving flushes).
Concurrent flush runs are safe under this assumption; late backfills that land in an
already-compacted partition are detected and re-merged on the next run (incremental state).
"""
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from shared.minio import BUCKET, get_s3fs

logger = logging.getLogger("archiver")

_MINIO_PREFIX = "silver/observations"

MAX_PARTITIONS = int(os.environ.get("COMPACT_SILVER_MAX_PARTITIONS", "10"))

SORT_COLS = ["make", "model", "dealer_state", "dealer_name", "year", "trim", "listing_id"]

_PART_RE = re.compile(r"^part-.*\.parquet$")
_COMPACTED_RE = re.compile(r"^compacted-.*\.parquet$")


def _today_utc() -> date:
    return datetime.now(timezone.utc).date()


def _parse_partition_path(path: str) -> Tuple[str, date]:
    """Extract (source, obs_date) from a hive partition path."""
    source = year = month = day = None
    for part in path.rstrip("/").split("/"):
        if part.startswith("source="):
            source = part[7:]
        elif part.startswith("obs_year="):
            year = int(part[9:])
        elif part.startswith("obs_month="):
            month = int(part[10:])
        elif part.startswith("obs_day="):
            day = int(part[8:])
    if None in (source, year, month, day):
        raise ValueError(f"Could not parse partition path: {path}")
    return source, date(year, month, day)


def _list_day_partitions(fs: Any, bucket: str) -> List[str]:
    """List all day-level partition directories under silver/observations."""
    base = f"{bucket}/{_MINIO_PREFIX}"
    day_paths: List[str] = []

    try:
        source_dirs = [e for e in fs.ls(base, detail=False) if "/source=" in e]
    except FileNotFoundError:
        return []

    for src_path in source_dirs:
        try:
            year_dirs = [e for e in fs.ls(src_path, detail=False) if "/obs_year=" in e]
        except FileNotFoundError:
            continue
        for year_path in year_dirs:
            try:
                month_dirs = [e for e in fs.ls(year_path, detail=False) if "/obs_month=" in e]
            except FileNotFoundError:
                continue
            for month_path in month_dirs:
                try:
                    day_dirs = [e for e in fs.ls(month_path, detail=False) if "/obs_day=" in e]
                except FileNotFoundError:
                    continue
                day_paths.extend(day_dirs)

    return day_paths


def _classify_partition(fs: Any, path: str) -> Tuple[str, List[str], List[str]]:
    """
    Classify a day partition by the files it contains.

    Returns (state, compacted_files, part_files) where state is one of:
      needs_compaction  — only part-*.parquet files present
      incremental       — both compacted-*.parquet and part-*.parquet present
      done              — only compacted-*.parquet present
      empty             — no parquet files at all
    """
    try:
        entries = fs.ls(path, detail=False)
    except FileNotFoundError:
        return "empty", [], []

    part_files: List[str] = []
    compacted_files: List[str] = []
    for entry in entries:
        name = entry.split("/")[-1]
        if _PART_RE.match(name):
            part_files.append(entry)
        elif _COMPACTED_RE.match(name):
            compacted_files.append(entry)

    if compacted_files and not part_files:
        return "done", compacted_files, []
    if part_files and not compacted_files:
        return "needs_compaction", [], part_files
    if part_files and compacted_files:
        return "incremental", compacted_files, part_files
    return "empty", [], []


def _compact_one(
    fs: Any,
    path: str,
    state: str,
    compacted_files: List[str],
    part_files: List[str],
    obs_date: date,
) -> Dict[str, Any]:
    """
    Compact a single partition directory using a reader-safe write sequence:
      1. Read all source files into memory
      2. Sort and write → compacted-<date>.parquet.tmp  (invisible to *.parquet glob)
      3. Assert tmp row count == source row count  ← pre-delete safety check
      4. Delete all originals  (partition is briefly empty — no double-count possible)
      5. fs.rename(tmp → compacted-<date>.parquet)

    If step 5 fails the .tmp is preserved for manual recovery; its path is logged at ERROR.
    """
    source, _ = _parse_partition_path(path)
    date_str = obs_date.strftime("%Y-%m-%d")

    read_files = (compacted_files + part_files) if state == "incremental" else part_files
    files_to_delete = (compacted_files + part_files) if state == "incremental" else part_files

    size_before = sum(fs.info(f)["size"] for f in read_files)

    if state == "incremental":
        logger.info(
            "compact_silver: incremental source=%s date=%s,"
            " %d new part files since last compaction",
            source, date_str, len(part_files),
        )

    # 1. Read all source files using ParquetFile to bypass dataset-level hive
    #    partition inference, which conflicts with columns stored in file data.
    tables = [pq.ParquetFile(f, filesystem=fs).read() for f in read_files]
    combined = pa.concat_tables(tables)
    expected_rows = len(combined)

    logger.info(
        "compact_silver: compacting source=%s date=%s, %d files, %d rows",
        source, date_str, len(read_files), expected_rows,
    )

    # 2. Sort (only on columns that exist in this partition's schema)
    sort_cols_present = [c for c in SORT_COLS if c in combined.schema.names]
    if sort_cols_present:
        combined = combined.sort_by([(c, "ascending") for c in sort_cols_present])

    # 3. Write to .tmp (not matched by *.parquet glob)
    tmp_path = f"{path}/compacted-{date_str}.parquet.tmp"
    pq.write_table(combined, tmp_path, filesystem=fs, compression="zstd")

    # 4. Assert row count before deleting originals
    written_rows = pq.ParquetFile(tmp_path, filesystem=fs).metadata.num_rows
    if written_rows != expected_rows:
        raise RuntimeError(
            f"Row count mismatch after write: expected {expected_rows}, got {written_rows}"
        )

    # 5. Delete originals (partition is empty during the rename that follows).
    # Any delete failure aborts the rename — leaving the .tmp unpublished prevents
    # double-counting with the surviving original files.
    delete_errors = []
    for f in files_to_delete:
        try:
            fs.rm(f)
        except Exception as e:
            logger.warning("compact_silver: failed to delete path=%s: %s", f, e)
            delete_errors.append((f, e))

    if delete_errors:
        raise RuntimeError(
            f"{len(delete_errors)} file(s) could not be deleted; aborting rename to prevent "
            f"double-counting. First error: {delete_errors[0][1]}"
        )

    # 6. Rename .tmp → final; on failure .tmp is left for manual recovery
    final_path = f"{path}/compacted-{date_str}.parquet"
    try:
        fs.rename(tmp_path, final_path)
    except Exception as e:
        logger.error(
            "compact_silver: rename failed, .tmp preserved for manual recovery: path=%s error=%s",
            tmp_path, e, exc_info=True,
        )
        raise

    size_after = fs.info(final_path)["size"]
    reduction_pct = round(100 * (size_before - size_after) / size_before) if size_before else 0

    logger.info(
        "compact_silver: done source=%s date=%s, before=%d bytes, after=%d bytes (-%d%%)",
        source, date_str, size_before, size_after, reduction_pct,
    )

    return {
        "source": source,
        "date": date_str,
        "state": state,
        "files_merged": len(read_files),
        "rows": expected_rows,
        "size_before_bytes": size_before,
        "size_after_bytes": size_after,
        "ok": True,
        "error": None,
    }


def compact_silver(max_partitions: int = MAX_PARTITIONS) -> Dict[str, Any]:
    """
    Discover and compact up to max_partitions completed silver observation partitions.

    Only processes obs_day <= today_utc - 2 (2-day watermark against late-arriving flushes).
    Concurrent flush runs are safe under the 2-day watermark assumption; late backfills that
    land in an already-compacted partition are handled by incremental compaction on the next run.

    Returns a summary dict plus a per-partition breakdown list.
    """
    try:
        fs = get_s3fs()
    except Exception as e:
        logger.error("compact_silver: MinIO connection failed: %s", e)
        return {
            "scanned": 0, "compacted": 0, "incremental": 0, "skipped": 0, "failed": 0,
            "size_before_mb": 0.0, "size_after_mb": 0.0,
            "error": str(e), "partitions": [],
        }

    watermark = _today_utc() - timedelta(days=2)

    # Discover all day-level partition directories
    all_day_paths = _list_day_partitions(fs, BUCKET)

    # Parse paths, apply watermark, classify state
    candidates = []
    skipped = 0

    for path in all_day_paths:
        try:
            source, obs_date = _parse_partition_path(path)
        except ValueError:
            continue
        if obs_date > watermark:
            continue

        state, compacted_files, part_files = _classify_partition(fs, path)

        if state == "done":
            logger.debug(
                "compact_silver: skip source=%s date=%s (already compacted)", source, obs_date
            )
            skipped += 1
            continue
        if state == "empty":
            continue

        candidates.append({
            "path": path,
            "source": source,
            "date": obs_date,
            "state": state,
            "compacted_files": compacted_files,
            "part_files": part_files,
        })

    # Sort oldest-first, then limit to max_partitions
    candidates.sort(key=lambda c: (c["date"], c["source"]))
    to_process = candidates[:max_partitions]

    # Compact each candidate
    compacted = incremental = failed = 0
    size_before_total = size_after_total = 0
    partition_results: List[Dict[str, Any]] = []

    for c in to_process:
        try:
            result = _compact_one(
                fs, c["path"], c["state"], c["compacted_files"], c["part_files"], c["date"]
            )
            partition_results.append(result)
            size_before_total += result["size_before_bytes"]
            size_after_total += result["size_after_bytes"]
            if c["state"] == "needs_compaction":
                compacted += 1
            else:
                incremental += 1
        except Exception as e:
            logger.error(
                "compact_silver: partition failed source=%s date=%s: %s",
                c["source"], c["date"], e, exc_info=True,
            )
            failed += 1
            partition_results.append({
                "source": c["source"],
                "date": c["date"].strftime("%Y-%m-%d"),
                "state": c["state"],
                "files_merged": 0,
                "rows": 0,
                "size_before_bytes": 0,
                "size_after_bytes": 0,
                "ok": False,
                "error": str(e),
            })

    logger.info(
        "compact_silver: run complete — compacted=%d incremental=%d failed=%d",
        compacted, incremental, failed,
    )

    return {
        "scanned": len(all_day_paths),
        "compacted": compacted,
        "incremental": incremental,
        "skipped": skipped,
        "failed": failed,
        "size_before_mb": round(size_before_total / (1024 * 1024), 2),
        "size_after_mb": round(size_after_total / (1024 * 1024), 2),
        "error": None,
        "partitions": partition_results,
    }
