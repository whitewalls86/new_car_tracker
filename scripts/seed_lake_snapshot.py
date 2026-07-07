"""
Seed a local/CI MinIO bucket from a downloaded Plan 120 lake snapshot.

    python scripts/download_lake_snapshot.py --latest
    python scripts/seed_lake_snapshot.py \\
        --snapshot .cache/lake_snapshots/adaptive-refresh-.../snapshot.tar.zst

Verifies the archive checksum against its manifest, safely unpacks it into a
temporary directory, and uploads Parquet/expected fixture files preserving
their archive-relative paths (the same prefixes dbt sources already read):

    silver_normalized/observations/...
    ops_normalized/price_observation_events/...
    ops_normalized/vin_to_listing_events/...
    ops_normalized/blocked_cooldown_events/...
    expected/...

Refuses to run against an endpoint or bucket that looks production-like
unless --allow-production-target is passed explicitly. Never defaults to
production credentials.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from scripts.lake_snapshot_common import (
    FIXTURE_PREFIXES,
    LakeSnapshotError,
    check_production_target,
    load_manifest,
    safe_extract_tar_zst,
    verify_archive_checksum,
)

DEFAULT_MINIO_ENDPOINT = "http://localhost:9000"
DEFAULT_BUCKET = "bronze"


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed a lake snapshot into MinIO (Plan 120)")
    parser.add_argument("--snapshot", dest="snapshot", required=True)
    parser.add_argument(
        "--manifest", dest="manifest", default=None,
        help="Explicit manifest path; defaults to manifest.json next to --snapshot",
    )
    parser.add_argument("--minio-endpoint", dest="minio_endpoint", default=DEFAULT_MINIO_ENDPOINT)
    parser.add_argument("--bucket", dest="bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--clear-prefixes", dest="clear_prefixes", action="store_true")
    parser.add_argument(
        "--allow-production-target", dest="allow_production_target", action="store_true",
    )
    return parser.parse_args(argv)


def build_boto3_client(endpoint: str):
    """
    Build a standalone boto3 client from CLI/env, without touching
    shared.minio's process-wide singleton or its ENDPOINT/BUCKET module
    globals (this is a script pointed at a possibly different target than
    the running services).
    """
    import boto3
    from botocore.client import Config

    access_key = os.environ.get("MINIO_ROOT_USER", "cartracker")
    secret_key = os.environ.get("MINIO_ROOT_PASSWORD", "")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def iter_upload_plan(extract_dir: Path) -> List[Tuple[str, Path]]:
    """Return (object_key, local_path) pairs for fixture files under extract_dir."""
    plan: List[Tuple[str, Path]] = []
    for prefix in FIXTURE_PREFIXES:
        root = extract_dir / prefix.rstrip("/")
        if not root.exists():
            continue
        for path in sorted(root.rglob("*")):
            if path.is_file():
                key = path.relative_to(extract_dir).as_posix()
                plan.append((key, path))
    return plan


def upload_files(
    client, bucket: str, plan: Iterable[Tuple[str, Path]],
) -> Dict[str, Dict[str, int]]:
    """Upload each (key, path) pair and return counts grouped by top-level prefix."""
    counts: Dict[str, Dict[str, int]] = {}
    for key, path in plan:
        top = key.split("/", 1)[0]
        size = path.stat().st_size
        client.upload_file(str(path), bucket, key)
        bucket_counts = counts.setdefault(top, {"files": 0, "bytes": 0})
        bucket_counts["files"] += 1
        bucket_counts["bytes"] += size
    return counts


def clear_fixture_prefixes(
    client, bucket: str, prefixes: Iterable[str] = FIXTURE_PREFIXES,
) -> int:
    """Delete all objects under known fixture prefixes in *bucket*. Returns count deleted."""
    deleted = 0
    for prefix in prefixes:
        continuation_token = None
        while True:
            kwargs: Dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token
            resp = client.list_objects_v2(**kwargs)
            keys = [{"Key": obj["Key"]} for obj in resp.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                deleted += len(keys)
            if resp.get("IsTruncated"):
                continuation_token = resp.get("NextContinuationToken")
            else:
                break
    return deleted


def seed_lake_snapshot(
    snapshot_path: Path,
    manifest_path: Optional[Path],
    minio_endpoint: str,
    bucket: str,
    clear_prefixes: bool,
    allow_production_target: bool,
    client=None,
) -> Dict[str, Any]:
    check_production_target(minio_endpoint, bucket, allow_production_target)

    snapshot_path = Path(snapshot_path)
    manifest_path = (
        Path(manifest_path) if manifest_path else snapshot_path.with_name("manifest.json")
    )
    if not manifest_path.exists():
        raise LakeSnapshotError(
            f"manifest not found at {manifest_path}; pass --manifest explicitly"
        )
    manifest = load_manifest(manifest_path)
    verify_archive_checksum(snapshot_path, manifest)

    if client is None:
        client = build_boto3_client(minio_endpoint)

    with tempfile.TemporaryDirectory(prefix="lake-snapshot-") as tmp:
        extract_dir = safe_extract_tar_zst(snapshot_path, Path(tmp))

        deleted = clear_fixture_prefixes(client, bucket) if clear_prefixes else 0

        plan = iter_upload_plan(extract_dir)
        counts = upload_files(client, bucket, plan)

    total_files = sum(c["files"] for c in counts.values())
    total_bytes = sum(c["bytes"] for c in counts.values())
    return {
        "bucket": bucket,
        "deleted_objects": deleted,
        "uploaded_by_prefix": counts,
        "uploaded_files": total_files,
        "uploaded_bytes": total_bytes,
    }


def main(argv: Optional[List[str]] = None) -> Dict[str, Any]:
    args = _parse_args(argv)
    result = seed_lake_snapshot(
        snapshot_path=Path(args.snapshot),
        manifest_path=Path(args.manifest) if args.manifest else None,
        minio_endpoint=args.minio_endpoint,
        bucket=args.bucket,
        clear_prefixes=args.clear_prefixes,
        allow_production_target=args.allow_production_target,
    )
    print(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    try:
        main()
    except LakeSnapshotError as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)
