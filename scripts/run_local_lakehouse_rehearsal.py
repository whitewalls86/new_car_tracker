"""
Plan 112 Gate A4: one-command local lakehouse rehearsal runner.

Orchestrates the full A4 flow (docs/runbook_lakehouse.md, A4 section) on a
dev box, consuming the Plan 120 Gate F ops download API for snapshot
refresh -- never SSH, never production MinIO/Postgres:

    python -m scripts.run_local_lakehouse_rehearsal

Steps (cache-aware; each is the same command the runbook documents):

    1. ensure the local analytics dir exists (default ./.cache/analytics)
    2. docker compose ... -p local-lakehouse up -d minio lakekeeper-postgres lakekeeper
    3. build the lakehouse-worker image (skip with --no-build-images)
    4. acquire a Plan 120 snapshot archive:
         --refresh-seed-data  -> download via Gate F (--latest or --snapshot-id)
         --snapshot-path      -> use an explicit local snapshot.tar.zst
         otherwise            -> newest cached .cache/lake_snapshots/*/snapshot.tar.zst
    5. seed the local MinIO (skipped when already seeded, unless
       --refresh-seed-data/--reseed-only force a clear+reseed)
    6. build analytics.duckdb via the dbt/Dockerfile image, targeted at
       +int_listing_volatility_features (no local Postgres needed; skipped
       when the file exists, unless --refresh-seed-data/--rebuild-duckdb)
    7. register the Lakekeeper warehouse (idempotent)
    8. python -m scripts.preflight_local_lakehouse_snapshot (must pass)
    9. A2 synthetic Iceberg roundtrip (skip with --skip-a2)
   10. A3 real-table rehearsal (skip with --skip-a3; --keep-iceberg-table
       passes --keep for debugging)

Safety posture: only the self-contained `local-lakehouse` Compose project is
ever touched (docker-compose.lakehouse.local.yml -- throwaway MinIO on
localhost:19000, non-external network). The local MinIO credentials
(cartracker/cartracker123, the local override's defaults) are passed
explicitly to every subprocess that needs them, so a production
MINIO_ROOT_PASSWORD in the parent shell or .env can never leak into the
local stack or its seeding. No `down`, no `-v`, nothing destructive.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import Callable, Dict, List, Mapping, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_BASE_URL = "https://cartracker.info"
DEFAULT_SNAPSHOT_DIR = ".cache/lake_snapshots"
DEFAULT_ANALYTICS_DIR = ".cache/analytics"
DEFAULT_PROJECT = "local-lakehouse"
DEFAULT_MINIO_ENDPOINT = "http://localhost:19000"
DEFAULT_DBT_IMAGE = "cartracker-dbt-local"
DEFAULT_BUCKET = "bronze"
TOKEN_ENV_VAR = "CARTRACKER_SNAPSHOT_TOKEN"

COMPOSE_FILES = ("docker-compose.lakehouse.yml", "docker-compose.lakehouse.local.yml")

# docker-compose.lakehouse.local.yml's throwaway-MinIO defaults. Passed
# explicitly to subprocesses (compose interpolation, seeding, the dbt
# container) so the local stack is deterministic regardless of what the
# parent shell or .env sets.
LOCAL_MINIO_ENV = {
    "MINIO_ROOT_USER": "cartracker",
    "MINIO_ROOT_PASSWORD": "cartracker123",
}

FEATURE_SELECT = "+int_listing_volatility_features"

RunnerFn = Callable[[List[str], Optional[Dict[str, str]]], int]


class RehearsalError(Exception):
    """Actionable orchestration failure (bad flags, missing snapshot, ...)."""


class StepFailed(Exception):
    def __init__(self, step: str, returncode: int):
        super().__init__(f"step {step!r} failed with exit code {returncode}")
        self.step = step
        self.returncode = returncode


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="One-command local lakehouse rehearsal (Plan 112 Gate A4)"
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="ops API base URL (Gate F)")
    parser.add_argument(
        "--token", default=None, help=f"Gate F bearer token; defaults to ${TOKEN_ENV_VAR}",
    )
    parser.add_argument(
        "--snapshot-id", default=None,
        help="Specific snapshot id to download (--refresh-seed-data) or reuse from the cache",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--refresh-seed-data", action="store_true",
        help="Download the latest/--snapshot-id snapshot via Gate F, clear+reseed "
             "local MinIO, and rebuild the local DuckDB",
    )
    mode.add_argument(
        "--reseed-only", action="store_true",
        help="Reuse an already-downloaded snapshot but clear+reseed local MinIO",
    )
    parser.add_argument(
        "--rebuild-duckdb", action="store_true",
        help="Rebuild local analytics.duckdb without redownloading/reseeding",
    )
    parser.add_argument("--skip-a2", action="store_true", help="Skip the A2 synthetic roundtrip")
    parser.add_argument("--skip-a3", action="store_true", help="Skip the A3 real-table rehearsal")
    parser.add_argument(
        "--keep-iceberg-table", action="store_true",
        help="Pass --keep to the A3 rehearsal (skips its cleanup, for debugging)",
    )
    parser.add_argument(
        "--no-build-images", action="store_true",
        help="Skip building the lakehouse-worker and dbt images",
    )
    parser.add_argument(
        "--snapshot-path", default=None,
        help="Explicit local snapshot.tar.zst (offline/manual use; manifest.json "
             "must sit beside it)",
    )
    parser.add_argument("--snapshot-dir", default=DEFAULT_SNAPSHOT_DIR)
    parser.add_argument("--analytics-dir", default=DEFAULT_ANALYTICS_DIR)
    parser.add_argument("--lakehouse-project", default=DEFAULT_PROJECT)
    parser.add_argument("--minio-endpoint", default=DEFAULT_MINIO_ENDPOINT)
    parser.add_argument("--dbt-image", default=DEFAULT_DBT_IMAGE)
    args = parser.parse_args(argv)
    if args.refresh_seed_data and args.snapshot_path:
        parser.error("--snapshot-path cannot be combined with --refresh-seed-data")
    return args


# ---------------------------------------------------------------------------
# Pure helpers (unit-tested without Docker)
# ---------------------------------------------------------------------------

def resolve_token(cli_token: Optional[str], env: Mapping[str, str]) -> Optional[str]:
    """CLI --token wins; fall back to $CARTRACKER_SNAPSHOT_TOKEN."""
    return cli_token or env.get(TOKEN_ENV_VAR) or None


def find_newest_snapshot(snapshot_dir: Path) -> Optional[Path]:
    """Newest .cache/lake_snapshots/<id>/snapshot.tar.zst by mtime, or None."""
    if not snapshot_dir.is_dir():
        return None
    candidates = sorted(
        snapshot_dir.glob("*/snapshot.tar.zst"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def compose_cmd(project: str, *tail: str) -> List[str]:
    cmd = ["docker", "compose"]
    for f in COMPOSE_FILES:
        cmd += ["-f", f]
    cmd += ["-p", project]
    cmd += list(tail)
    return cmd


def build_stack_up_command(project: str) -> List[str]:
    return compose_cmd(project, "up", "-d", "minio", "lakekeeper-postgres", "lakekeeper")


def build_worker_build_command(project: str) -> List[str]:
    return compose_cmd(project, "build", "lakehouse-worker")


def build_worker_run_command(project: str, module: str, *module_args: str) -> List[str]:
    return compose_cmd(
        project, "run", "--rm", "lakehouse-worker", "python", "-m", module, *module_args,
    )


def build_seed_command(
    snapshot_path: Path, minio_endpoint: str, clear_prefixes: bool,
) -> List[str]:
    cmd = [
        sys.executable, "-m", "scripts.seed_lake_snapshot",
        "--snapshot", str(snapshot_path),
        "--minio-endpoint", minio_endpoint,
    ]
    if clear_prefixes:
        cmd.append("--clear-prefixes")
    return cmd


def build_dbt_image_command(dbt_image: str) -> List[str]:
    return ["docker", "build", "-f", "dbt/Dockerfile", "-t", dbt_image, "."]


def build_dbt_run_command(analytics_dir: Path, project: str, dbt_image: str) -> List[str]:
    """Targeted dbt build against the seeded local MinIO. POSTGRES_URL is a
    dummy on purpose: +int_listing_volatility_features selects no
    postgres_scan() source, so no local Postgres is required."""
    return [
        "docker", "run", "--rm",
        "--network", f"{project}_cartracker-net",
        "-e", "DUCKDB_PATH=/out/analytics.duckdb",
        "-e", "MINIO_ENDPOINT=http://minio:9000",
        "-e", f"MINIO_ROOT_USER={LOCAL_MINIO_ENV['MINIO_ROOT_USER']}",
        "-e", f"MINIO_ROOT_PASSWORD={LOCAL_MINIO_ENV['MINIO_ROOT_PASSWORD']}",
        "-e", "MINIO_BUCKET=bronze",
        "-e", "POSTGRES_URL=postgresql://unused:unused@localhost:5432/unused",
        "-v", f"{analytics_dir.resolve()}:/out",
        dbt_image,
        "build", "--target", "duckdb", "--full-refresh", "--select", FEATURE_SELECT,
    ]


def build_preflight_command(
    minio_endpoint: str, analytics_path: Path, manifest_path: Path,
) -> List[str]:
    return [
        sys.executable, "-m", "scripts.preflight_local_lakehouse_snapshot",
        "--minio-endpoint", minio_endpoint,
        "--analytics-path", str(analytics_path),
        "--manifest", str(manifest_path),
    ]


# ---------------------------------------------------------------------------
# Default collaborators (subprocess/network; injectable for tests)
# ---------------------------------------------------------------------------

def _stream_run(cmd: List[str], env_overrides: Optional[Dict[str, str]] = None) -> int:
    """Run *cmd* from the repo root, inheriting stdio so long dbt/Spark steps
    stream live. Always an explicit args list, never a shell."""
    env = {**os.environ, **env_overrides} if env_overrides else None
    print(f"+ {' '.join(cmd)}", flush=True)
    return subprocess.run(cmd, cwd=str(REPO_ROOT), env=env).returncode


def _default_downloader(args: argparse.Namespace, token: str) -> Path:
    from scripts.download_lake_snapshot import download_api
    from scripts.lake_snapshot_common import LakeSnapshotError

    try:
        return Path(
            download_api(
                base_url=args.base_url,
                token=token,
                latest=args.snapshot_id is None,
                snapshot_id=args.snapshot_id,
                out_dir=Path(args.snapshot_dir),
            )
        )
    except LakeSnapshotError as e:
        raise RehearsalError(f"Gate F download failed: {e}") from e


def _local_boto3_client(endpoint: str):
    import boto3
    from botocore.client import Config

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=LOCAL_MINIO_ENV["MINIO_ROOT_USER"],
        aws_secret_access_key=LOCAL_MINIO_ENV["MINIO_ROOT_PASSWORD"],
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _default_seeded_checker(endpoint: str) -> bool:
    """True when every required fixture prefix already has objects (reuses the
    preflight's read-only check). Any error means 'not seeded'."""
    try:
        from scripts.preflight_local_lakehouse_snapshot import PASS, check_snapshot_seeded

        client = _local_boto3_client(endpoint)
        return check_snapshot_seeded(endpoint, DEFAULT_BUCKET, client=client).status == PASS
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def acquire_snapshot(
    args: argparse.Namespace,
    downloader: Callable[[argparse.Namespace, str], Path],
    note: Callable[[str, str], None],
) -> Path:
    snapshot_dir = Path(args.snapshot_dir)

    if args.refresh_seed_data:
        token = resolve_token(args.token, os.environ)
        if not token:
            raise RehearsalError(
                f"--refresh-seed-data downloads via the Gate F API and needs a "
                f"token: pass --token or set ${TOKEN_ENV_VAR}"
            )
        path = downloader(args, token)
        note("snapshot", f"downloaded {path}")
        return path

    if args.snapshot_path:
        path = Path(args.snapshot_path)
        if not path.exists():
            raise RehearsalError(f"--snapshot-path {path} does not exist")
        note("snapshot", f"using {path} (--snapshot-path)")
        return path

    if args.snapshot_id:
        path = snapshot_dir / args.snapshot_id / "snapshot.tar.zst"
        if not path.exists():
            raise RehearsalError(
                f"snapshot {args.snapshot_id!r} is not in the cache ({path}); "
                f"download it with --refresh-seed-data --snapshot-id {args.snapshot_id}"
            )
        note("snapshot", f"using cached {path}")
        return path

    newest = find_newest_snapshot(snapshot_dir)
    if newest is None:
        raise RehearsalError(
            f"no snapshot found under {snapshot_dir} -- download one first:\n"
            f"  python -m scripts.run_local_lakehouse_rehearsal --refresh-seed-data\n"
            f"(or manually: python -m scripts.download_lake_snapshot --latest "
            f"--base-url {args.base_url})"
        )
    note("snapshot", f"using newest cached {newest}")
    return newest


def execute(
    args: argparse.Namespace,
    runner: RunnerFn = _stream_run,
    downloader: Callable[[argparse.Namespace, str], Path] = _default_downloader,
    seeded_checker: Callable[[str], bool] = _default_seeded_checker,
) -> int:
    summary: List[Tuple[str, str]] = []

    def note(step: str, status: str) -> None:
        summary.append((step, status))

    def run_step(step: str, cmd: List[str], env: Optional[Dict[str, str]] = None) -> None:
        code = runner(cmd, env)
        if code != 0:
            note(step, f"FAILED (exit {code})")
            raise StepFailed(step, code)
        note(step, "ok")

    project = args.lakehouse_project
    analytics_dir = Path(args.analytics_dir)
    duckdb_path = analytics_dir / "analytics.duckdb"
    exit_code = 0

    try:
        analytics_dir.mkdir(parents=True, exist_ok=True)

        run_step("stack-up", build_stack_up_command(project), env=LOCAL_MINIO_ENV)

        if args.no_build_images:
            note("worker-image", "skipped (--no-build-images)")
        else:
            run_step("worker-image", build_worker_build_command(project), env=LOCAL_MINIO_ENV)

        snapshot_path = acquire_snapshot(args, downloader, note)

        force_seed = args.refresh_seed_data or args.reseed_only
        if not force_seed and seeded_checker(args.minio_endpoint):
            note("seed", "skipped (already seeded)")
        else:
            run_step(
                "seed",
                build_seed_command(snapshot_path, args.minio_endpoint, clear_prefixes=force_seed),
                env=LOCAL_MINIO_ENV,
            )

        need_duckdb = args.refresh_seed_data or args.rebuild_duckdb or not duckdb_path.exists()
        if not need_duckdb:
            note("duckdb-build", f"skipped (cached {duckdb_path})")
        else:
            if args.no_build_images:
                note("dbt-image", "skipped (--no-build-images)")
            else:
                run_step("dbt-image", build_dbt_image_command(args.dbt_image))
            run_step(
                "duckdb-build",
                build_dbt_run_command(analytics_dir, project, args.dbt_image),
            )

        # scripts.register_lakehouse_warehouse is idempotent end to end --
        # already-bootstrapped/already-registered exits 0.
        run_step(
            "warehouse",
            build_worker_run_command(project, "scripts.register_lakehouse_warehouse"),
            env=LOCAL_MINIO_ENV,
        )

        run_step(
            "preflight",
            build_preflight_command(
                args.minio_endpoint, duckdb_path, snapshot_path.with_name("manifest.json"),
            ),
            env=LOCAL_MINIO_ENV,
        )

        if args.skip_a2:
            note("a2-roundtrip", "skipped (--skip-a2)")
        else:
            run_step(
                "a2-roundtrip",
                build_worker_run_command(project, "scripts.spike_iceberg_lakehouse", "roundtrip"),
                env=LOCAL_MINIO_ENV,
            )

        if args.skip_a3:
            note("a3-rehearsal", "skipped (--skip-a3)")
        else:
            a3_args = ["rehearsal"]
            if args.keep_iceberg_table:
                a3_args.append("--keep")
            run_step(
                "a3-rehearsal",
                build_worker_run_command(
                    project, "scripts.export_volatility_features_to_iceberg", *a3_args,
                ),
                env=LOCAL_MINIO_ENV,
            )
    except StepFailed as e:
        print(f"\nerror: {e}", file=sys.stderr)
        exit_code = e.returncode or 1
    except RehearsalError as e:
        print(f"\nerror: {e}", file=sys.stderr)
        exit_code = 1

    print("\n== A4 local rehearsal summary ==")
    width = max((len(step) for step, _ in summary), default=0)
    for step, status in summary:
        print(f"{step.ljust(width)}  {status}")
    if exit_code == 0:
        print("\nAll steps completed.")
    return exit_code


def main(argv: Optional[List[str]] = None) -> int:
    return execute(_parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
