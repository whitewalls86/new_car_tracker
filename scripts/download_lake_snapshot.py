"""
Download a Plan 120 CI/local lake snapshot.

Two modes:

  API mode (Phase 2/3 ops download surface):
      python scripts/download_lake_snapshot.py --latest \\
          --base-url https://cartracker.info --token $CARTRACKER_SNAPSHOT_TOKEN
      python scripts/download_lake_snapshot.py \\
          --snapshot-id adaptive-refresh-2026-07-07-174500 \\
          --base-url https://cartracker.info

  Local/offline mode (works before the ops download routes exist, and for
  tests/local dev against a hand-built or archiver-generated manifest+archive
  pair on disk):
      python scripts/download_lake_snapshot.py \\
          --manifest-path .cache/build/manifest.json \\
          --archive-path .cache/build/snapshot.tar.zst

Both modes verify the archive sha256 against the manifest before writing it
to --out, and never silently accept a checksum mismatch.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from scripts.lake_snapshot_common import (
    LakeSnapshotError,
    get_archive_meta,
    load_manifest,
    verify_archive_checksum,
)

DEFAULT_OUT_DIR = ".cache/lake_snapshots"
TOKEN_ENV_VAR = "CARTRACKER_SNAPSHOT_TOKEN"

_SNAPSHOTS_PATH = "/admin/snapshots/adaptive-refresh"


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download a lake snapshot (Plan 120)")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--latest", action="store_true", help="Download the latest snapshot")
    mode.add_argument(
        "--snapshot-id", dest="snapshot_id", default=None,
        help="Download a specific snapshot id, e.g. adaptive-refresh-2026-07-07-174500",
    )
    parser.add_argument("--base-url", dest="base_url", default=None, help="ops API base URL")
    parser.add_argument(
        "--token", dest="token", default=None,
        help=f"Bearer token; defaults to ${TOKEN_ENV_VAR}",
    )
    parser.add_argument("--out", dest="out", default=DEFAULT_OUT_DIR)
    parser.add_argument(
        "--manifest-path", dest="manifest_path", default=None,
        help="Local manifest.json to use instead of the ops API",
    )
    parser.add_argument(
        "--archive-path", dest="archive_path", default=None,
        help="Local snapshot.tar.zst to use instead of the ops API "
             "(defaults to snapshot.tar.zst next to --manifest-path)",
    )
    return parser.parse_args(argv)


def _finalize_archive(tmp_path: Path, dest_path: Path, manifest: Dict[str, Any]) -> Path:
    """Verify a staged .tmp archive against the manifest, then atomically
    promote it to dest_path. Removes the .tmp file on checksum failure so a
    bad archive is never left in the cache under its final name."""
    try:
        verify_archive_checksum(tmp_path, manifest)
    except LakeSnapshotError:
        tmp_path.unlink(missing_ok=True)
        raise
    os.replace(tmp_path, dest_path)
    return dest_path


def _write_snapshot(
    manifest: Dict[str, Any], src_archive: Path, out_dir: Path,
) -> Path:
    snapshot_id = manifest.get("snapshot_id") or "snapshot"
    dest_dir = out_dir / snapshot_id
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_archive = dest_dir / "snapshot.tar.zst"
    tmp_archive = dest_dir / "snapshot.tar.zst.tmp"
    shutil.copyfile(src_archive, tmp_archive)

    _finalize_archive(tmp_archive, dest_archive, manifest)

    dest_manifest = dest_dir / "manifest.json"
    with open(dest_manifest, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)

    return dest_archive


def download_local(
    manifest_path: Path, archive_path: Optional[Path], out_dir: Path,
) -> Path:
    """Verify/copy a snapshot from local manifest+archive paths (no HTTP)."""
    manifest_path = Path(manifest_path)
    manifest = load_manifest(manifest_path)

    if archive_path is None:
        archive_meta = get_archive_meta(manifest)
        archive_path = manifest_path.parent / archive_meta["path"]
    archive_path = Path(archive_path)
    if not archive_path.exists():
        raise LakeSnapshotError(f"archive not found at {archive_path}")

    return _write_snapshot(manifest, archive_path, out_dir)


def download_api(
    base_url: Optional[str],
    token: Optional[str],
    latest: bool,
    snapshot_id: Optional[str],
    out_dir: Path,
    client=None,
) -> Path:
    """Fetch manifest + archive from the ops download API (Plan 120 Phase 2/3)."""
    if not base_url:
        raise LakeSnapshotError("--base-url is required in API mode")
    if not token:
        raise LakeSnapshotError(
            f"a token is required in API mode: pass --token or set ${TOKEN_ENV_VAR}"
        )
    if not latest and not snapshot_id:
        raise LakeSnapshotError("API mode requires --latest or --snapshot-id")

    owns_client = client is None
    if owns_client:
        import httpx
        client = httpx.Client(
            base_url=base_url, headers={"Authorization": f"Bearer {token}"}, timeout=60.0,
        )

    try:
        if latest:
            resp = client.get(f"{_SNAPSHOTS_PATH}/latest")
            resp.raise_for_status()
            snapshot_id = resp.json()["snapshot_id"]

        resp = client.get(f"{_SNAPSHOTS_PATH}/{snapshot_id}")
        resp.raise_for_status()
        manifest = resp.json()

        dest_dir = out_dir / snapshot_id
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_archive = dest_dir / "snapshot.tar.zst"
        tmp_archive = dest_dir / "snapshot.tar.zst.tmp"

        with client.stream("GET", f"{_SNAPSHOTS_PATH}/{snapshot_id}/download") as stream:
            stream.raise_for_status()
            with open(tmp_archive, "wb") as fh:
                for chunk in stream.iter_bytes():
                    fh.write(chunk)
    finally:
        if owns_client:
            client.close()

    _finalize_archive(tmp_archive, dest_archive, manifest)
    with open(dest_dir / "manifest.json", "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)

    return dest_archive


def main(argv: Optional[List[str]] = None) -> str:
    args = _parse_args(argv)
    out_dir = Path(args.out)

    local_mode = bool(args.manifest_path or args.archive_path)
    if local_mode:
        if not args.manifest_path:
            raise LakeSnapshotError(
                "--manifest-path is required in local mode "
                "(pass it alongside --archive-path)"
            )
        archive_path = Path(args.archive_path) if args.archive_path else None
        dest_archive = download_local(Path(args.manifest_path), archive_path, out_dir)
    else:
        if not args.latest and not args.snapshot_id:
            raise LakeSnapshotError(
                "one of --latest, --snapshot-id, or --manifest-path is required"
            )
        token = args.token or os.environ.get(TOKEN_ENV_VAR)
        dest_archive = download_api(
            args.base_url, token, args.latest, args.snapshot_id, out_dir,
        )

    print(str(dest_archive))
    return str(dest_archive)


if __name__ == "__main__":
    try:
        main()
    except LakeSnapshotError as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)
