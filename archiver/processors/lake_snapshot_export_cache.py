"""
Export fingerprint and materialized-export cache for CI lake snapshot exports
(Plan 120 Gate D).

Mirrors `lake_snapshot_planning_cache.py`'s fingerprint/cache pattern, but for
a different question: given an already-planned cohort (identified by a
`planning_fingerprint`), which Parquet fixture files represent that cohort
under the *current* export rules (included tables, writer algorithm, output
schema, partition layout, compression, sanitization)? The export fingerprint
lets an equivalent export request reuse a previously materialized dataset
without re-filtering production Parquet.

The export semantics hashed here are code-level constants (not
`SnapshotRequest` fields) — this first pass writer has no user-configurable
output format, so the only thing that can change what bytes get written is
either the planning fingerprint (a different cohort) or a code change to this
module's constants (a different writer version).
"""
import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from shared.minio import read_json, write_json

logger = logging.getLogger("archiver")

EXPORT_CACHE_SCHEMA_VERSION = 2
EXPORT_ALGORITHM_VERSION = 1
OUTPUT_SCHEMA_VERSION = 1
PARTITION_LAYOUT_VERSION = 1
PARQUET_COMPRESSION = "zstd"

DEFAULT_EXPORT_PREFIX = "snapshot_exports"

# Logical table -> MinIO prefix, mirroring lake_source_audit.SOURCE_TABLE_SPECS
# relative paths. Kept as an explicit list here (rather than importing that
# module's dict) so the fingerprint hashes exactly what this writer includes,
# independent of the source-audit module's own evolution.
INCLUDED_TABLES: Tuple[str, ...] = (
    "silver_observations",
    "price_observation_events",
    "vin_to_listing_events",
    "blocked_cooldown_events",
)


def _hash_json(payload: Dict[str, Any]) -> str:
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def compute_export_fingerprint(planning_fingerprint: str) -> Tuple[str, Dict[str, Any]]:
    """Compute the export fingerprint for the current writer semantics.

    Excludes snapshot_id, dry_run, and other run labels that do not change
    output bytes — the same exclusion rationale as
    `lake_snapshot_planning_cache.compute_planning_fingerprint`.
    """
    payload = {
        "export_cache_schema_version": EXPORT_CACHE_SCHEMA_VERSION,
        "planning_fingerprint": planning_fingerprint,
        "included_tables": list(INCLUDED_TABLES),
        "export_algorithm_version": EXPORT_ALGORITHM_VERSION,
        "output_schema_version": OUTPUT_SCHEMA_VERSION,
        "partition_layout_version": PARTITION_LAYOUT_VERSION,
        "parquet_compression": PARQUET_COMPRESSION,
    }
    return _hash_json(payload), payload


def export_manifest_path(prefix: str, fingerprint: str) -> str:
    return f"{prefix.rstrip('/')}/fingerprints/{fingerprint}/manifest.json"


def build_export_manifest(
    *,
    fingerprint: str,
    planning_fingerprint: str,
    export_fingerprint_payload: Dict[str, Any],
    snapshot_id: str,
    tier: str,
    source_window: Dict[str, Any],
    counts: Dict[str, int],
    coverage: Dict[str, Any],
    tables: Dict[str, Any],
    data_path: str,
    generation_id: str,
) -> Dict[str, Any]:
    """Build the manifest for a materialized export.

    `tables` should map logical table name -> {"path", "rows", "files",
    "sha256", "error"} once the writer has actually written that table.
    `data_path`/`generation_id` identify the specific immutable generation
    directory (see `lake_snapshot_export.materialize_filtered_tables`) this
    manifest publishes — writing this manifest is the sole "publish" step;
    the generation directory itself is never mutated in place.
    """
    return {
        "export_cache_schema_version": EXPORT_CACHE_SCHEMA_VERSION,
        "export_fingerprint": fingerprint,
        "planning_fingerprint": planning_fingerprint,
        "export_fingerprint_payload": export_fingerprint_payload,
        "snapshot_id": snapshot_id,
        "tier": tier,
        "source_window": source_window,
        "counts": counts,
        "coverage": coverage,
        "tables": tables,
        "data_path": data_path,
        "generation_id": generation_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def _manifest_incompleteness_reason(
    manifest: Dict[str, Any], expected_fingerprint: str,
) -> Optional[str]:
    """Return a reason string if `manifest` is not a complete, trustworthy
    materialized export for `expected_fingerprint`, else None.

    Deliberately does not re-verify file existence/checksums against MinIO —
    that would require re-listing every object on every cache hit, defeating
    the purpose of caching. This checks only what's cheaply knowable from the
    manifest JSON itself: it's for the fingerprint the caller asked for, every
    required table is present, and no table recorded a read/write error
    (`lake_snapshot_export.materialize_filtered_tables` never promotes a
    partial result, but this is defense-in-depth against a manifest written
    by an older/buggy version that did)."""
    if manifest.get("export_fingerprint") != expected_fingerprint:
        return (
            f"export_fingerprint mismatch: manifest has "
            f"{manifest.get('export_fingerprint')!r}, expected {expected_fingerprint!r}"
        )
    if not manifest.get("data_path"):
        return "missing data_path"
    tables = manifest.get("tables") or {}
    missing = [name for name in INCLUDED_TABLES if name not in tables]
    if missing:
        return f"missing tables: {missing}"
    errored = [name for name in INCLUDED_TABLES if tables[name].get("error")]
    if errored:
        return f"tables with recorded errors: {errored}"
    return None


def load_export_manifest(path: str, expected_fingerprint: str) -> Optional[Dict[str, Any]]:
    """Load a materialized export manifest, or None on a miss, schema
    mismatch, or incomplete/untrustworthy manifest (see
    `_manifest_incompleteness_reason`)."""
    logger.info("lake_snapshot_export_cache: lookup start path=%s", path)
    try:
        manifest = read_json(path)
    except Exception as e:
        logger.warning("lake_snapshot_export_cache: load failed path=%s error=%s", path, e)
        return None
    if manifest is None:
        logger.info("lake_snapshot_export_cache: miss path=%s", path)
        return None
    if manifest.get("export_cache_schema_version") != EXPORT_CACHE_SCHEMA_VERSION:
        logger.warning(
            "lake_snapshot_export_cache: schema mismatch path=%s cached_version=%s "
            "expected_version=%s; treating as miss",
            path, manifest.get("export_cache_schema_version"), EXPORT_CACHE_SCHEMA_VERSION,
        )
        return None
    incomplete_reason = _manifest_incompleteness_reason(manifest, expected_fingerprint)
    if incomplete_reason is not None:
        logger.warning(
            "lake_snapshot_export_cache: incomplete manifest path=%s reason=%s; "
            "treating as miss",
            path, incomplete_reason,
        )
        return None
    logger.info("lake_snapshot_export_cache: hit path=%s", path)
    return manifest


def write_export_manifest(path: str, manifest: Dict[str, Any]) -> bool:
    """Persist a materialized export manifest. Never raises — returns False
    on failure instead, so the caller (the manifest write is the actual
    "publish" step for a materialized export) can distinguish a failed
    publish from a successful one rather than assuming success."""
    t0 = time.monotonic()
    try:
        write_json(path, manifest)
        logger.info(
            "lake_snapshot_export_cache: write ok path=%s elapsed_s=%.2f",
            path, time.monotonic() - t0,
        )
        return True
    except Exception as e:
        logger.warning(
            "lake_snapshot_export_cache: write failed path=%s elapsed_s=%.2f error=%s",
            path, time.monotonic() - t0, e,
        )
        return False
