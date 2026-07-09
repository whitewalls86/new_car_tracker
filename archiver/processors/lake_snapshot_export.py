"""
Fingerprint-addressed filtered Parquet writer for CI lake snapshot exports
(Plan 120 Gate D).

Given a closed cohort (VINs, listing_ids, and explicit artifact row keys from
`lake_snapshot_cohort.SnapshotCohort`), reads the four supported production
source tables and writes a filtered, dbt-compatible fixture dataset to a
fingerprint-addressed MinIO prefix.

Filter semantics (see docs/plan_120_ci_lake_snapshot_delivery.md and the
Gate D design notes): `artifact_id` is never used as a blanket
`artifact_id IN (...)` filter against an entity table — a single artifact_id
(e.g. an SRP/carousel page) can legitimately span many rows for different,
unrelated VINs, and filtering on it that way would silently reintroduce rows
the cohort closure deliberately excluded. Instead, artifact-only-seeded rows
(currently just `invalid_or_null_vin`) are matched by their exact
`(artifact_id, vin, listing_id)` row identity, captured during selector
candidate collection as `CandidateSet.selected_row_keys`.
"""
import glob
import hashlib
import logging
import os
import shutil
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, FrozenSet, List, Optional, Tuple

import pyarrow.parquet as pq

from archiver.processors.lake_snapshot_sql import in_clause, table_time_where
from archiver.processors.lake_source_audit import resolve_table_path
from shared.minio import BUCKET, get_s3fs

logger = logging.getLogger("archiver")

# table -> timestamp column, partition columns, relative MinIO prefix, and
# stable sort keys. Sort keys make row ordering (and therefore file bytes)
# deterministic across runs, independent of source file enumeration order —
# required for reproducible checksums.
_TABLE_WRITE_SPECS: Dict[str, Dict[str, Any]] = {
    "silver_observations": {
        "ts_col": "fetched_at",
        "partition_cols": ["source", "obs_year", "obs_month"],
        "relative_prefix": "silver_normalized/observations",
        "sort_keys": ["fetched_at", "listing_id", "artifact_id"],
    },
    "price_observation_events": {
        "ts_col": "event_at",
        "partition_cols": ["year", "month"],
        "relative_prefix": "ops_normalized/price_observation_events",
        "sort_keys": ["event_at", "listing_id", "artifact_id", "event_id"],
    },
    "vin_to_listing_events": {
        "ts_col": "event_at",
        "partition_cols": ["year", "month"],
        "relative_prefix": "ops_normalized/vin_to_listing_events",
        "sort_keys": ["event_at", "listing_id", "artifact_id", "event_id"],
    },
    "blocked_cooldown_events": {
        "ts_col": "event_at",
        "partition_cols": ["year", "month"],
        "relative_prefix": "ops_normalized/blocked_cooldown_events",
        "sort_keys": ["event_at", "listing_id", "event_id"],
    },
}


def _artifact_row_key_where(
    row_keys: FrozenSet[Tuple[Any, Any, Any]],
) -> Tuple[str, List[Any]]:
    """Build an exact-match OR-of-ANDs predicate for explicit artifact row
    keys. NULL-safe (`IS NOT DISTINCT FROM`) because invalid_or_null_vin rows
    can have a NULL vin."""
    if not row_keys:
        return "FALSE", []
    parts: List[str] = []
    params: List[Any] = []
    for artifact_id, vin, listing_id in row_keys:
        parts.append(
            "(artifact_id = ? AND vin IS NOT DISTINCT FROM ? "
            "AND listing_id IS NOT DISTINCT FROM ?)"
        )
        params += [artifact_id, vin, listing_id]
    return f"({' OR '.join(parts)})", params


def _build_table_query(
    table_name: str,
    path: str,
    window_start: Optional[datetime],
    window_end: Optional[datetime],
    vins: FrozenSet[str],
    listing_ids: FrozenSet[str],
    artifact_row_keys: FrozenSet[Tuple[Any, Any, Any]],
) -> Tuple[str, List[Any]]:
    spec = _TABLE_WRITE_SPECS[table_name]
    or_parts: List[str] = []
    params: List[Any] = []

    if table_name == "blocked_cooldown_events":
        # This table has no vin column; listing_id membership only.
        where_sql, params = in_clause("listing_id", listing_ids)
    else:
        vin_clause, vin_params = in_clause("vin", vins)
        or_parts.append(vin_clause)
        params += vin_params

        listing_clause, listing_params = in_clause("listing_id", listing_ids)
        or_parts.append(listing_clause)
        params += listing_params

        if table_name == "silver_observations":
            row_key_clause, row_key_params = _artifact_row_key_where(artifact_row_keys)
            or_parts.append(row_key_clause)
            params += row_key_params

        where_sql = f"({' OR '.join(or_parts)})"

    clauses = [where_sql]
    time_clauses, time_params = table_time_where(window_start, window_end, spec["ts_col"])
    clauses += time_clauses
    params += time_params

    order_by = ", ".join(spec["sort_keys"])
    query = (
        f"SELECT * FROM read_parquet('{path}', union_by_name=true, hive_partitioning=true) "
        f"WHERE {' AND '.join(clauses)} ORDER BY {order_by}"
    )
    return query, params


def _sha256_files(filesystem, paths: List[str]) -> List[Dict[str, Any]]:
    results = []
    for path in sorted(paths):
        with filesystem.open(path, "rb") as f:
            digest = hashlib.sha256(f.read()).hexdigest()
        results.append({"path": path, "sha256": digest})
    return results


def _write_table(
    con,
    table_name: str,
    base_path: Optional[str],
    window_start: Optional[datetime],
    window_end: Optional[datetime],
    vins: FrozenSet[str],
    listing_ids: FrozenSet[str],
    artifact_row_keys: FrozenSet[Tuple[Any, Any, Any]],
    output_root: str,
) -> Dict[str, Any]:
    """Filter and write one logical table. Returns its manifest entry."""
    spec = _TABLE_WRITE_SPECS[table_name]
    path = resolve_table_path(table_name, base_path)
    t0 = time.monotonic()
    logger.info("lake_snapshot_export: table=%s start", table_name)

    query, params = _build_table_query(
        table_name, path, window_start, window_end, vins, listing_ids, artifact_row_keys,
    )
    try:
        arrow_table = con.execute(query, params).to_arrow_table()
    except Exception as e:
        logger.warning(
            "lake_snapshot_export: table=%s error elapsed_s=%.2f path=%s error=%s",
            table_name, time.monotonic() - t0, path, e,
        )
        return {
            "path": spec["relative_prefix"], "rows": 0, "files": 0, "sha256": [], "error": str(e),
        }
    rows = arrow_table.num_rows

    table_root = f"{output_root.rstrip('/')}/{spec['relative_prefix']}"
    files: List[Dict[str, Any]] = []
    if rows > 0:
        if base_path:
            pq.write_to_dataset(
                arrow_table,
                root_path=table_root,
                partition_cols=spec["partition_cols"],
                existing_data_behavior="overwrite_or_ignore",
                basename_template="part-{i}.parquet",
            )
            written_paths = glob.glob(f"{table_root}/**/*.parquet", recursive=True)
            files = []
            for p in sorted(written_paths):
                with open(p, "rb") as f:
                    files.append({"path": p, "sha256": hashlib.sha256(f.read()).hexdigest()})
            files_count = len(written_paths)
        else:
            fs = get_s3fs()
            pq.write_to_dataset(
                arrow_table,
                root_path=f"s3://{table_root}",
                partition_cols=spec["partition_cols"],
                filesystem=fs,
                compression="zstd",
                existing_data_behavior="overwrite_or_ignore",
                basename_template="part-{i}.parquet",
            )
            written_paths = fs.find(f"{table_root}")
            files = _sha256_files(fs, written_paths)
            files_count = len(written_paths)
    else:
        files_count = 0

    logger.info(
        "lake_snapshot_export: table=%s end elapsed_s=%.2f rows=%d files=%d",
        table_name, time.monotonic() - t0, rows, files_count,
    )
    return {
        "path": spec["relative_prefix"],
        "rows": rows,
        "files": files_count,
        "sha256": [f["sha256"] for f in files],
        "error": None,
    }


def _remove_prefix(root: str, base_path: Optional[str]) -> None:
    if base_path:
        if os.path.isdir(root):
            shutil.rmtree(root)
    else:
        fs = get_s3fs()
        if fs.exists(root):
            fs.rm(root, recursive=True)


def _write_success_marker(root: str, base_path: Optional[str]) -> None:
    """Write an empty `_SUCCESS` object at `root` — a cheap, independently
    checkable signal (for future GC/audit tooling) that a generation
    directory was fully written, without needing to parse or trust
    manifest.json. Not read on the cache-hit hot path; manifest.json's own
    per-table error fields already gate reuse there (see
    `lake_snapshot_export_cache.load_export_manifest`)."""
    marker_path = f"{root.rstrip('/')}/_SUCCESS"
    if base_path:
        os.makedirs(root, exist_ok=True)
        with open(marker_path, "w"):
            pass
    else:
        get_s3fs().pipe_file(marker_path, b"")


@dataclass
class MaterializationResult:
    tables: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    generation_id: Optional[str] = None
    data_path: Optional[str] = None

    @property
    def ok(self) -> bool:
        return all(t.get("error") is None for t in self.tables.values())


def materialize_filtered_tables(
    con,
    base_path: Optional[str],
    window_start: Optional[datetime],
    window_end: Optional[datetime],
    vins: FrozenSet[str],
    listing_ids: FrozenSet[str],
    artifact_row_keys: FrozenSet[Tuple[Any, Any, Any]],
    export_fingerprint: str,
    export_prefix: str,
) -> MaterializationResult:
    """Filter and write all four supported tables for the given cohort.

    Writes to a fresh, immutable, uniquely-named generation directory
    (`{export_prefix}/fingerprints/{export_fingerprint}/generations/{generation_id}/data`,
    or the equivalent local path when `base_path` is set for fixture-mode
    tests) — never overwriting or deleting any previously published
    generation. This sidesteps the atomicity hazard of promoting in place
    (delete-then-copy can leave an already-published manifest pointing at
    missing/partial data mid-copy, or forever if the copy fails): every
    generation this function writes is either fully present at its own path
    or was never referenced by any manifest, because the caller is
    responsible for only writing/updating `manifest.json` — the single small
    object that actually "publishes" a generation — after confirming every
    table succeeded (`MaterializationResult.ok`).

    On any table error, the partial generation directory is removed
    (best-effort; harmless either way since nothing references it yet) and
    `ok` is False. Orphaned generations from failed or superseded runs are
    not cleaned up here — that's future GC/audit tooling, aided by the
    `_SUCCESS` marker written into every fully-succeeded generation.
    """
    # 12 hex chars (48 bits) is ample collision resistance for infrequent,
    # per-fingerprint writes, and keeps the generation-scoped path shorter —
    # deeply nested hive-partitioned paths can otherwise approach Windows'
    # legacy 260-char MAX_PATH in local fixture-mode tests.
    generation_id = uuid.uuid4().hex[:12]
    generation_relative = (
        f"{export_prefix}/fingerprints/{export_fingerprint}/generations/{generation_id}"
    )
    data_relative = f"{generation_relative}/data"
    if base_path:
        generation_root = f"{base_path.rstrip('/')}/{generation_relative}"
        data_root = f"{base_path.rstrip('/')}/{data_relative}"
    else:
        generation_root = f"{BUCKET}/{generation_relative}"
        data_root = f"{BUCKET}/{data_relative}"

    t0 = time.monotonic()
    logger.info(
        "lake_snapshot_export: materialize_filtered_tables start "
        "export_fingerprint=%s generation_id=%s vins=%d listing_ids=%d artifact_row_keys=%d",
        export_fingerprint, generation_id, len(vins), len(listing_ids), len(artifact_row_keys),
    )
    tables: Dict[str, Dict[str, Any]] = {}
    for table_name in _TABLE_WRITE_SPECS:
        tables[table_name] = _write_table(
            con, table_name, base_path, window_start, window_end,
            vins, listing_ids, artifact_row_keys, data_root,
        )

    errors = {name: t["error"] for name, t in tables.items() if t["error"] is not None}
    if errors:
        logger.warning(
            "lake_snapshot_export: materialize_filtered_tables aborting elapsed_s=%.2f "
            "export_fingerprint=%s generation_id=%s errors=%s",
            time.monotonic() - t0, export_fingerprint, generation_id, errors,
        )
        _remove_prefix(generation_root, base_path)
        return MaterializationResult(tables=tables, generation_id=generation_id)

    _write_success_marker(generation_root, base_path)
    logger.info(
        "lake_snapshot_export: materialize_filtered_tables end elapsed_s=%.2f "
        "generation_id=%s total_rows=%d",
        time.monotonic() - t0, generation_id, sum(t["rows"] for t in tables.values()),
    )
    return MaterializationResult(
        tables=tables, generation_id=generation_id, data_path=data_relative,
    )
