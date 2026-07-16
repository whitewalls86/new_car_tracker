"""
Plan 112 Gate A4: preflight checks for the local lakehouse harness.

Read-only sanity checks for the "fresh clone to local lakehouse smoke" path
(docs/plan_112_refresh_policy_backtesting.md, A4 section) so missing dependencies, an unseeded
MinIO, a stale/absent analytics.duckdb, or an unregistered Iceberg warehouse
are caught with an actionable message *before* a Spark container run fails
half an hour in. Run it from the repo root on the host:

    python scripts/preflight_local_lakehouse_snapshot.py

Defaults target the docker-compose.lakehouse.local.yml stack (MinIO on
localhost:19000, Lakekeeper on localhost:18181, analytics.duckdb under
./.cache/analytics, Plan 120 snapshot downloads under ./.cache/lake_snapshots).
Every default can be overridden by flag; see --help.

Checks (in order; later checks are SKIPped when a prerequisite failed):

    required-files      repo files the A4 flow needs (compose overrides,
                        lakehouse/Dockerfile, seed/export scripts)
    snapshot-archive    a Plan 120 manifest.json + snapshot.tar.zst pair is
                        present (size-verified; --verify-checksum for sha256)
    minio-endpoint      endpoint is not production-like and answers health/live
    snapshot-seeded     the fixture prefixes hold at least one object
    analytics-duckdb    analytics.duckdb exists and is non-empty
    feature-tables      required dbt feature tables exist in the DuckDB file
                        (at minimum int_listing_volatility_features)
    lakekeeper          management /management/v1/info answers
    warehouse           the Iceberg warehouse is registered (catalog
                        /v1/config?warehouse=... answers 200)

This script never writes, deletes, or registers anything -- it only reads.
It refuses (fails the minio-endpoint check) if the MinIO endpoint or bucket
looks production-like; there is deliberately no override flag for that here.
Heavy clients (httpx, boto3, duckdb) are imported lazily inside their checks
so a missing optional dependency degrades to one actionable FAIL, and so
unit tests can run without any of them installed.
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from scripts.lake_snapshot_common import (
    FIXTURE_PREFIXES,
    LakeSnapshotError,
    ProductionTargetError,
    check_production_target,
    get_archive_meta,
    load_manifest,
    verify_archive_checksum,
)
from shared.iceberg_catalog import WAREHOUSE_NAME

DEFAULT_MINIO_ENDPOINT = "http://localhost:19000"
DEFAULT_BUCKET = "bronze"
DEFAULT_LAKEKEEPER_URL = "http://localhost:18181"
DEFAULT_SNAPSHOT_DIR = ".cache/lake_snapshots"
DEFAULT_ANALYTICS_PATH = ".cache/analytics/analytics.duckdb"
DEFAULT_REQUIRED_TABLES = ("int_listing_volatility_features",)

# scripts/lake_snapshot_common.FIXTURE_PREFIXES also lists "expected/", but
# the actual Gate D/E export (archiver/processors/lake_snapshot_export.py)
# only ever materializes/packages the four source-table prefixes below --
# "expected/" is an aspirational fixture-shape note in
# docs/plan_120_ci_lake_snapshot_delivery.md that nothing currently writes.
# Requiring it here would make a real, correctly-seeded VM-generated archive
# fail this check forever. Seed verification only checks what the harness
# actually needs: the dbt sources under silver_normalized/ and ops_normalized/.
REQUIRED_SEED_PREFIXES = tuple(p for p in FIXTURE_PREFIXES if p != "expected/")

REQUIRED_FILES = (
    "docker-compose.lakehouse.yml",
    "docker-compose.lakehouse.local.yml",
    "lakehouse/Dockerfile",
    "lakehouse/requirements.txt",
    "scripts/download_lake_snapshot.py",
    "scripts/seed_lake_snapshot.py",
    "scripts/register_lakehouse_warehouse.py",
    "scripts/export_volatility_features_to_iceberg.py",
    "shared/iceberg_catalog.py",
)

PASS = "PASS"
FAIL = "FAIL"
WARN = "WARN"
SKIP = "SKIP"


@dataclass
class CheckResult:
    name: str
    status: str
    message: str

    @property
    def failed(self) -> bool:
        return self.status == FAIL


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Preflight checks for the local lakehouse harness (Plan 112 Gate A4)"
    )
    parser.add_argument("--minio-endpoint", default=DEFAULT_MINIO_ENDPOINT)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--lakekeeper-url", default=DEFAULT_LAKEKEEPER_URL)
    parser.add_argument(
        "--snapshot-dir", default=DEFAULT_SNAPSHOT_DIR,
        help="Directory scripts/download_lake_snapshot.py wrote snapshots into",
    )
    parser.add_argument(
        "--manifest", default=None,
        help="Explicit manifest.json path (skips --snapshot-dir discovery)",
    )
    parser.add_argument(
        "--analytics-path", default=DEFAULT_ANALYTICS_PATH,
        help="Local analytics.duckdb path (the file mounted read-only by "
             "docker-compose.lakehouse.local.yml)",
    )
    parser.add_argument(
        "--required-table", dest="required_tables", action="append", default=None,
        help="dbt feature table that must exist in the DuckDB file; repeatable "
             f"(default: {', '.join(DEFAULT_REQUIRED_TABLES)})",
    )
    parser.add_argument(
        "--verify-checksum", action="store_true",
        help="Also verify the snapshot archive's full sha256 (slower)",
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Individual checks -- each returns a CheckResult and never raises.
# ---------------------------------------------------------------------------

def check_required_files(repo_root: Path) -> CheckResult:
    missing = [f for f in REQUIRED_FILES if not (repo_root / f).exists()]
    if missing:
        return CheckResult(
            "required-files", FAIL,
            f"missing: {', '.join(missing)} -- run from the repo root of a "
            "complete checkout",
        )
    return CheckResult("required-files", PASS, f"all {len(REQUIRED_FILES)} present")


def find_snapshot_manifest(snapshot_dir: Path) -> Optional[Path]:
    """Return the most recently modified manifest.json under *snapshot_dir*
    (the layout scripts/download_lake_snapshot.py writes), or None."""
    if not snapshot_dir.is_dir():
        return None
    manifests = sorted(
        snapshot_dir.rglob("manifest.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return manifests[0] if manifests else None


def check_snapshot_archive(
    snapshot_dir: Path, manifest_path: Optional[Path], verify_checksum: bool,
) -> CheckResult:
    if manifest_path is None:
        manifest_path = find_snapshot_manifest(snapshot_dir)
        if manifest_path is None:
            return CheckResult(
                "snapshot-archive", FAIL,
                f"no manifest.json found under {snapshot_dir} -- download a "
                "Plan 120 snapshot first: python scripts/download_lake_snapshot.py "
                "--manifest-path <archive_manifest.json> (see runbook A4 section)",
            )
    try:
        manifest = load_manifest(manifest_path)
        meta = get_archive_meta(manifest)
    except LakeSnapshotError as e:
        return CheckResult("snapshot-archive", FAIL, f"{manifest_path}: {e}")

    archive_path = manifest_path.parent / "snapshot.tar.zst"
    if not archive_path.exists():
        return CheckResult(
            "snapshot-archive", FAIL,
            f"manifest found at {manifest_path} but no snapshot.tar.zst beside it",
        )
    declared_bytes = meta.get("bytes")
    actual_bytes = archive_path.stat().st_size
    if declared_bytes is not None and actual_bytes != declared_bytes:
        return CheckResult(
            "snapshot-archive", FAIL,
            f"{archive_path} is {actual_bytes} bytes but the manifest declares "
            f"{declared_bytes} -- re-download the snapshot",
        )
    if verify_checksum:
        try:
            verify_archive_checksum(archive_path, manifest)
        except LakeSnapshotError as e:
            return CheckResult("snapshot-archive", FAIL, str(e))
        return CheckResult(
            "snapshot-archive", PASS, f"{archive_path} (sha256 verified)"
        )
    return CheckResult("snapshot-archive", PASS, f"{archive_path} (size verified)")


def check_minio_endpoint(endpoint: str, bucket: str) -> CheckResult:
    try:
        check_production_target(endpoint, bucket, allow_production_target=False)
    except ProductionTargetError as e:
        return CheckResult(
            "minio-endpoint", FAIL,
            f"{e} -- this preflight only ever targets a local/throwaway MinIO",
        )
    try:
        import httpx
    except ImportError:
        return CheckResult(
            "minio-endpoint", FAIL,
            "httpx is not installed -- pip install -r requirements-dev.txt",
        )
    health_url = f"{endpoint.rstrip('/')}/minio/health/live"
    try:
        resp = httpx.get(health_url, timeout=5.0)
        resp.raise_for_status()
    except Exception as e:
        return CheckResult(
            "minio-endpoint", FAIL,
            f"{health_url} unreachable ({e}) -- start the local stack: "
            "docker compose -f docker-compose.lakehouse.yml "
            "-f docker-compose.lakehouse.local.yml -p local-lakehouse "
            "up -d minio lakekeeper-postgres lakekeeper",
        )
    return CheckResult("minio-endpoint", PASS, endpoint)


def check_snapshot_seeded(endpoint: str, bucket: str, client=None) -> CheckResult:
    if client is None:
        try:
            from scripts.seed_lake_snapshot import build_boto3_client
            client = build_boto3_client(endpoint)
        except ImportError as e:
            return CheckResult(
                "snapshot-seeded", FAIL,
                f"boto3 unavailable ({e}) -- pip install -r requirements-dev.txt",
            )
    empty = []
    try:
        for prefix in REQUIRED_SEED_PREFIXES:
            resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            if resp.get("KeyCount", 0) == 0:
                empty.append(prefix)
    except Exception as e:
        return CheckResult(
            "snapshot-seeded", FAIL, f"listing bucket {bucket!r} failed: {e}"
        )
    if empty:
        return CheckResult(
            "snapshot-seeded", FAIL,
            f"no objects under {', '.join(empty)} in bucket {bucket!r} -- seed "
            "the snapshot: python scripts/seed_lake_snapshot.py --snapshot "
            "<snapshot.tar.zst> --minio-endpoint " + endpoint,
        )
    return CheckResult(
        "snapshot-seeded", PASS,
        f"objects present under all of {', '.join(REQUIRED_SEED_PREFIXES)}",
    )


def check_analytics_duckdb(analytics_path: Path) -> CheckResult:
    if not analytics_path.exists():
        return CheckResult(
            "analytics-duckdb", FAIL,
            f"{analytics_path} not found -- build it with dbt against the "
            "seeded MinIO (runbook A4 'Build analytics.duckdb' step), or copy "
            "an existing analytics.duckdb there",
        )
    if analytics_path.stat().st_size == 0:
        return CheckResult(
            "analytics-duckdb", FAIL, f"{analytics_path} exists but is empty"
        )
    return CheckResult("analytics-duckdb", PASS, str(analytics_path))


def check_feature_tables(analytics_path: Path, required_tables: List[str]) -> CheckResult:
    try:
        import duckdb
    except ImportError:
        return CheckResult(
            "feature-tables", FAIL,
            "duckdb is not installed on the host -- pip install duckdb, or "
            "verify inside the lakehouse-worker container instead",
        )
    try:
        con = duckdb.connect(str(analytics_path), read_only=True)
    except Exception as e:
        return CheckResult(
            "feature-tables", FAIL, f"cannot open {analytics_path} read-only: {e}"
        )
    try:
        rows = con.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall()
    finally:
        con.close()
    present = {row[0] for row in rows}
    missing = [t for t in required_tables if t not in present]
    if missing:
        return CheckResult(
            "feature-tables", FAIL,
            f"missing table(s) in {analytics_path}: {', '.join(missing)} -- "
            "the dbt build is stale or incomplete; rebuild against the seeded "
            "MinIO (runbook A4 section)",
        )
    return CheckResult("feature-tables", PASS, f"found: {', '.join(required_tables)}")


def check_lakekeeper(lakekeeper_url: str) -> CheckResult:
    try:
        import httpx
    except ImportError:
        return CheckResult(
            "lakekeeper", FAIL,
            "httpx is not installed -- pip install -r requirements-dev.txt",
        )
    info_url = f"{lakekeeper_url.rstrip('/')}/management/v1/info"
    try:
        resp = httpx.get(info_url, timeout=5.0)
        resp.raise_for_status()
    except Exception as e:
        return CheckResult(
            "lakekeeper", FAIL,
            f"{info_url} unreachable ({e}) -- start the local stack: "
            "docker compose -f docker-compose.lakehouse.yml "
            "-f docker-compose.lakehouse.local.yml -p local-lakehouse "
            "up -d minio lakekeeper-postgres lakekeeper",
        )
    return CheckResult("lakekeeper", PASS, info_url)


def check_warehouse_registered(lakekeeper_url: str, warehouse: str) -> CheckResult:
    try:
        import httpx
    except ImportError:
        return CheckResult(
            "warehouse", FAIL,
            "httpx is not installed -- pip install -r requirements-dev.txt",
        )
    config_url = f"{lakekeeper_url.rstrip('/')}/catalog/v1/config"
    try:
        resp = httpx.get(config_url, params={"warehouse": warehouse}, timeout=5.0)
    except Exception as e:
        return CheckResult("warehouse", FAIL, f"{config_url} unreachable ({e})")
    if resp.status_code != 200:
        return CheckResult(
            "warehouse", FAIL,
            f"warehouse {warehouse!r} not registered (catalog /v1/config "
            f"returned {resp.status_code}) -- register it: docker compose "
            "-f docker-compose.lakehouse.yml -f docker-compose.lakehouse.local.yml "
            "-p local-lakehouse run --rm lakehouse-worker "
            "python -m scripts.register_lakehouse_warehouse",
        )
    return CheckResult("warehouse", PASS, f"{warehouse!r} registered")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def _skip(name: str, reason: str) -> CheckResult:
    return CheckResult(name, SKIP, reason)


def run_preflight(args: argparse.Namespace, repo_root: Path) -> List[CheckResult]:
    """Run every check in dependency order. Never raises; returns the full
    result list (SKIPping checks whose prerequisite failed)."""
    results: List[CheckResult] = []
    results.append(check_required_files(repo_root))

    manifest_path = Path(args.manifest) if args.manifest else None
    results.append(
        check_snapshot_archive(Path(args.snapshot_dir), manifest_path, args.verify_checksum)
    )

    minio_result = check_minio_endpoint(args.minio_endpoint, args.bucket)
    results.append(minio_result)
    if minio_result.failed:
        results.append(_skip("snapshot-seeded", "minio-endpoint failed"))
    else:
        results.append(check_snapshot_seeded(args.minio_endpoint, args.bucket))

    analytics_path = Path(args.analytics_path)
    duckdb_result = check_analytics_duckdb(analytics_path)
    results.append(duckdb_result)
    required_tables = args.required_tables or list(DEFAULT_REQUIRED_TABLES)
    if duckdb_result.failed:
        results.append(_skip("feature-tables", "analytics-duckdb failed"))
    else:
        results.append(check_feature_tables(analytics_path, required_tables))

    lakekeeper_result = check_lakekeeper(args.lakekeeper_url)
    results.append(lakekeeper_result)
    if lakekeeper_result.failed:
        results.append(_skip("warehouse", "lakekeeper failed"))
    else:
        results.append(check_warehouse_registered(args.lakekeeper_url, WAREHOUSE_NAME))

    return results


def format_results(results: List[CheckResult]) -> str:
    width = max(len(r.name) for r in results)
    lines = [f"{r.name.ljust(width)}  {r.status:<4}  {r.message}" for r in results]
    failed = sum(1 for r in results if r.failed)
    lines.append("")
    if failed:
        lines.append(f"{failed} check(s) FAILED -- fix the first failure and re-run.")
    else:
        lines.append("All checks passed -- ready for the A4 local rehearsal.")
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    repo_root = Path(__file__).resolve().parent.parent
    results = run_preflight(args, repo_root)
    print(format_results(results))
    return 1 if any(r.failed for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
