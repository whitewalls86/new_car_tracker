"""
Seed a local/CI MinIO bucket and Postgres from a downloaded Plan 120 lake
snapshot.

    python scripts/download_lake_snapshot.py --latest
    python scripts/seed_lake_snapshot.py \\
        --snapshot .cache/lake_snapshots/adaptive-refresh-.../snapshot.tar.zst \\
        --postgres-url postgresql://cartracker:cartracker@localhost:5432/cartracker

Verifies the archive checksum against its manifest, safely unpacks it into a
temporary directory, and uploads Parquet/expected fixture files preserving
their archive-relative paths (the same prefixes dbt sources already read):

    silver_normalized/observations/...
    ops_normalized/price_observation_events/...
    ops_normalized/vin_to_listing_events/...
    ops_normalized/blocked_cooldown_events/...
    expected/...

Plan 162 Stage P added a second destination. `dbt/models/sources.yml` declares
six source tables; the two that resolve through `postgres_scan()` cannot be
objects in a bucket, so they travel as JSON under `postgres/` and are written
into a database instead. That half runs only when --postgres-url is given, and
`--require-non-empty` is what turns "the snapshot seeded nothing" from a green
build over an empty world into a failure at the seed.

Refuses to run against an endpoint, bucket or Postgres host that looks
production-like unless --allow-production-target is passed explicitly. Never
defaults to production credentials. The Postgres half of that guard is stricter
than the MinIO half -- see `lake_snapshot_common.is_production_like_postgres_url`.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# ``python scripts/seed_lake_snapshot.py`` puts ``scripts/`` rather than the
# repository root on sys.path.  Keep the documented direct invocation working
# while retaining package imports for tests and ``python -m`` usage.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.lake_snapshot_common import (
    FIXTURE_PREFIXES,
    LakeSnapshotError,
    check_production_target,
    load_manifest,
    safe_extract_tar_zst,
    verify_archive_checksum,
)
from shared.lake_snapshot_postgres import (
    POSTGRES_PREFIX,
    POSTGRES_SNAPSHOT_TABLES,
    load_table,
    parse_snapshot_object_name,
)

DEFAULT_MINIO_ENDPOINT = "http://localhost:9000"
DEFAULT_BUCKET = "bronze"

# The four lake tables a complete snapshot carries, as the archive manifest
# names them. Used only by --require-non-empty; the upload plan itself walks the
# extracted tree and needs no list.
LAKE_TABLES = (
    "silver_observations",
    "price_observation_events",
    "vin_to_listing_events",
    "blocked_cooldown_events",
)


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
    parser.add_argument(
        "--postgres-url", dest="postgres_url", default="",
        help="Load the snapshot's postgres/ members into this database. Omitted, "
             "they are skipped and reported in the result.",
    )
    parser.add_argument(
        "--require-non-empty", dest="require_non_empty", action="store_true",
        help="Fail if any of the six dbt sources seeded zero rows. Without it a "
             "short snapshot seeds quietly and the build that reads it is green "
             "over an empty world.",
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


def ensure_bucket(client, bucket: str) -> None:
    """Create *bucket* if it doesn't exist yet (fresh local/CI MinIO instances)."""
    from botocore.exceptions import ClientError

    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code not in ("404", "NoSuchBucket"):
            raise
        client.create_bucket(Bucket=bucket)


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


def iter_postgres_plan(extract_dir: Path) -> List[Tuple[str, str, Path]]:
    """Return (schema, table, local_path) for every `postgres/` archive member.

    Order follows POSTGRES_SNAPSHOT_TABLES rather than the filesystem, and a
    member the allowlist does not name raises rather than being skipped: a
    snapshot carrying a table this seeder cannot place is a snapshot the seed
    would silently under-apply, which is the failure the caller is trying to
    rule out.
    """
    root = extract_dir / POSTGRES_PREFIX.rstrip("/")
    if not root.exists():
        return []

    present = {
        path.relative_to(extract_dir).as_posix()
        for path in root.rglob("*") if path.is_file()
    }
    plan: List[Tuple[str, str, Path]] = []
    for name in sorted(present):
        # Raises UnknownSnapshotTableError on anything outside the allowlist.
        schema, table = parse_snapshot_object_name(name)
        plan.append((schema, table, extract_dir / name))
    order = {(s, t): i for i, (s, t) in enumerate(POSTGRES_SNAPSHOT_TABLES)}
    plan.sort(key=lambda entry: order[(entry[0], entry[1])])
    return plan


def load_postgres_tables(
    postgres_url: str, plan: Iterable[Tuple[str, str, Path]], conn=None,
) -> Dict[str, int]:
    """Replace each planned table's contents; returns rows written per table.

    One connection, one transaction: a snapshot is a set of tables that agree
    with each other, so a half-applied one is worse than none.
    """
    plan = list(plan)
    if not plan:
        return {}

    owns_conn = conn is None
    if owns_conn:
        import psycopg2
        conn = psycopg2.connect(postgres_url)
    try:
        counts: Dict[str, int] = {}
        with conn:
            with conn.cursor() as cur:
                for schema, table, path in plan:
                    rows_json = path.read_text(encoding="utf-8")
                    counts[f"{schema}.{table}"] = load_table(cur, schema, table, rows_json)
        return counts
    finally:
        if owns_conn:
            conn.close()


def assert_non_empty(
    manifest: Dict[str, Any], postgres_counts: Dict[str, int],
) -> None:
    """Raise unless all six dbt sources landed at least one row.

    The four lake tables are read from the manifest rather than counted from the
    uploaded files: a Parquet file with zero rows is still a file, so a
    file-count check would pass exactly where this has to fail.
    """
    tables = manifest.get("tables") or {}
    empty = [
        f"{name} ({(tables.get(name) or {}).get('rows', 0)} rows)"
        for name in LAKE_TABLES
        if not (tables.get(name) or {}).get("rows")
    ]
    empty += [
        f"{schema}.{table} ({postgres_counts.get(f'{schema}.{table}', 0)} rows)"
        for schema, table in POSTGRES_SNAPSHOT_TABLES
        if not postgres_counts.get(f"{schema}.{table}")
    ]
    if empty:
        raise LakeSnapshotError(
            "--require-non-empty: these dbt sources seeded no rows, so a build "
            "over this seed would be green over an empty world: " + ", ".join(empty)
        )


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
    postgres_url: str = "",
    require_non_empty: bool = False,
    client=None,
    conn=None,
) -> Dict[str, Any]:
    check_production_target(
        minio_endpoint, bucket, allow_production_target, postgres_url=postgres_url,
    )

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
    ensure_bucket(client, bucket)

    with tempfile.TemporaryDirectory(prefix="lake-snapshot-") as tmp:
        extract_dir = safe_extract_tar_zst(snapshot_path, Path(tmp))

        deleted = clear_fixture_prefixes(client, bucket) if clear_prefixes else 0

        plan = iter_upload_plan(extract_dir)
        counts = upload_files(client, bucket, plan)

        postgres_plan = iter_postgres_plan(extract_dir)
        if postgres_url:
            postgres_counts = load_postgres_tables(postgres_url, postgres_plan, conn=conn)
            postgres_skipped: List[str] = []
        else:
            postgres_counts = {}
            postgres_skipped = [f"{schema}.{table}" for schema, table, _ in postgres_plan]

    if require_non_empty:
        assert_non_empty(manifest, postgres_counts)

    total_files = sum(c["files"] for c in counts.values())
    total_bytes = sum(c["bytes"] for c in counts.values())
    return {
        "bucket": bucket,
        "deleted_objects": deleted,
        "uploaded_by_prefix": counts,
        "uploaded_files": total_files,
        "uploaded_bytes": total_bytes,
        "postgres_rows_by_table": postgres_counts,
        "postgres_skipped": postgres_skipped,
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
        postgres_url=args.postgres_url,
        require_non_empty=args.require_non_empty,
    )
    print(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    try:
        main()
    except LakeSnapshotError as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)
