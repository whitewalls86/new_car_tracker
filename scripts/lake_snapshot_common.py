"""
Shared helpers for the Plan 120 Phase 4 local dev lake snapshot scripts:

  scripts/download_lake_snapshot.py
  scripts/seed_lake_snapshot.py

Kept dependency-light and side-effect-free so both scripts (and their tests)
can share checksum, manifest, safe-extraction, and production-target-guard
logic without duplicating it.
"""
from __future__ import annotations

import hashlib
import json
import tarfile
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlparse


class LakeSnapshotError(Exception):
    """Base error for lake snapshot download/seed failures."""


class ChecksumMismatchError(LakeSnapshotError):
    """Raised when an archive's sha256 does not match its manifest."""


class ProductionTargetError(LakeSnapshotError):
    """Raised when a seed target looks production-like without an explicit override."""


# What the seeder uploads to MinIO. `postgres/` is deliberately absent: those
# members are rows for a database, not objects for a bucket, and the seeder has
# to be able to tell them apart by path before it opens anything. See
# shared/lake_snapshot_postgres.POSTGRES_PREFIX.
FIXTURE_PREFIXES = ("silver_normalized/", "ops_normalized/", "expected/")

DEFAULT_ARCHIVE_NAME = "snapshot.tar.zst"

_PRODUCTION_HOST_MARKERS = ("cartracker.info", "147.224.199.86")
_LOCAL_HOSTS = {"localhost", "127.0.0.1", "0.0.0.0", "minio"}
# The Postgres guard's allowlist. No Compose service names -- see
# is_production_like_postgres_url for why that half is stricter.
_LOOPBACK_HOSTS = {"localhost", "127.0.0.1", "::1"}
_PRODUCTION_BUCKET_MARKERS = ("prod",)


# ---------------------------------------------------------------------------
# Checksums / manifest
# ---------------------------------------------------------------------------

def sha256_file(path: Path, chunk_size: int = 1 << 20) -> str:
    """Return the hex sha256 digest of the file at *path*."""
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_manifest(path: Path) -> Dict[str, Any]:
    """Load and parse a manifest.json file."""
    path = Path(path)
    if not path.exists():
        raise LakeSnapshotError(f"manifest not found at {path}")
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def get_archive_meta(manifest: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return {"sha256", "bytes", "path"} from a manifest, tolerating both the
    rich contract (manifest["archive"] = {...}) and a flatter skeleton shape
    (manifest["archive_sha256"] at the top level).

    Raises LakeSnapshotError if no checksum can be found in either shape.
    """
    archive = manifest.get("archive")
    if isinstance(archive, dict) and archive.get("sha256"):
        return {
            "sha256": archive["sha256"],
            "bytes": archive.get("bytes"),
            "path": archive.get("path") or DEFAULT_ARCHIVE_NAME,
        }

    top_level_sha256 = manifest.get("archive_sha256")
    if top_level_sha256:
        return {
            "sha256": top_level_sha256,
            "bytes": manifest.get("archive_bytes"),
            "path": manifest.get("archive_path") or DEFAULT_ARCHIVE_NAME,
        }

    raise LakeSnapshotError(
        "manifest does not contain an archive checksum "
        "(expected archive.sha256 or archive_sha256)"
    )


def verify_archive_checksum(archive_path: Path, manifest: Dict[str, Any]) -> str:
    """
    Verify *archive_path*'s sha256 matches the manifest's recorded checksum.

    Returns the verified hex digest. Raises ChecksumMismatchError on mismatch,
    never silently accepting a bad archive.
    """
    archive_path = Path(archive_path)
    if not archive_path.exists():
        raise LakeSnapshotError(f"archive not found at {archive_path}")

    expected = get_archive_meta(manifest)["sha256"]
    actual = sha256_file(archive_path)
    if actual != expected:
        raise ChecksumMismatchError(
            f"checksum mismatch for {archive_path}: expected {expected}, got {actual}"
        )
    return actual


# ---------------------------------------------------------------------------
# Safe tar.zst extraction
# ---------------------------------------------------------------------------

def _is_safe_member(member: tarfile.TarInfo, dest_dir: Path) -> bool:
    if member.issym() or member.islnk():
        return False
    member_path = (dest_dir / member.name).resolve()
    try:
        member_path.relative_to(dest_dir)
    except ValueError:
        return False
    return True


def safe_extract_tar_zst(archive_path: Path, dest_dir: Path) -> Path:
    """
    Decompress and extract a .tar.zst archive into *dest_dir*, rejecting any
    member that would traverse outside dest_dir (via "../" paths, absolute
    paths, or symlinks/hardlinks).

    Returns dest_dir. Raises LakeSnapshotError on the first unsafe member.
    """
    import zstandard

    archive_path = Path(archive_path)
    dest_dir = Path(dest_dir).resolve()
    dest_dir.mkdir(parents=True, exist_ok=True)

    dctx = zstandard.ZstdDecompressor()
    with open(archive_path, "rb") as fh:
        with dctx.stream_reader(fh) as reader:
            with tarfile.open(fileobj=reader, mode="r|") as tar:
                for member in tar:
                    if not _is_safe_member(member, dest_dir):
                        raise LakeSnapshotError(
                            f"refusing to extract unsafe archive member: {member.name}"
                        )
                    tar.extract(member, path=dest_dir, filter="data")
    return dest_dir


# ---------------------------------------------------------------------------
# Production-target guard
# ---------------------------------------------------------------------------

def is_production_like_endpoint(endpoint: str) -> bool:
    """Return True if *endpoint* looks like the production MinIO deployment."""
    lowered = (endpoint or "").lower()
    if any(marker in lowered for marker in _PRODUCTION_HOST_MARKERS):
        return True

    host = (urlparse(endpoint).hostname or "").lower()
    if host in _LOCAL_HOSTS:
        return False

    import ipaddress
    try:
        if ipaddress.ip_address(host).is_private:
            return False
    except ValueError:
        pass  # not a literal IP address (e.g. a hostname) — fall through

    # Unknown/public host: treat conservatively as production-like.
    return True


def is_production_like_bucket(bucket: str) -> bool:
    lowered = (bucket or "").lower()
    return any(marker in lowered for marker in _PRODUCTION_BUCKET_MARKERS)


def is_production_like_postgres_url(postgres_url: str) -> bool:
    """Return True unless *postgres_url* points at a loopback or private-IP host.

    **Deliberately stricter than the MinIO guard above**, which treats a bare
    Compose service name (``minio``) as dev-shaped. Plan 162 Stage P gave the
    seeder a Postgres write path, and a tool that can ``INSERT`` into any
    connection string it is handed is a different risk class from one that can
    only upload Parquet: the MinIO half writes fixture objects under known
    prefixes, while this half runs a ``DELETE`` against a live operational
    table. So a hostname is not enough here -- only a loopback literal or a
    private IP passes, and ``postgres:5432`` inside a Compose network (which
    *is* production, from inside production) is refused rather than waved
    through by resembling a service name.

    An unparseable or empty URL is production-like, for the same fail-safe
    reason an unknown public host is.
    """
    host = (urlparse(postgres_url or "").hostname or "").lower()
    if not host:
        return True
    if host in _LOOPBACK_HOSTS:
        return False

    import ipaddress
    try:
        return not ipaddress.ip_address(host).is_private
    except ValueError:
        # A hostname, not a literal address. Unknown by construction.
        return True


def check_production_target(
    endpoint: str,
    bucket: str,
    allow_production_target: bool,
    postgres_url: str = "",
) -> None:
    """
    Raise ProductionTargetError if *endpoint*, *bucket* or *postgres_url* look
    production-like and *allow_production_target* is not set.
    """
    if allow_production_target:
        return
    if is_production_like_endpoint(endpoint):
        raise ProductionTargetError(
            f"refusing to seed: MinIO endpoint '{endpoint}' looks production-like; "
            "pass --allow-production-target to override"
        )
    if is_production_like_bucket(bucket):
        raise ProductionTargetError(
            f"refusing to seed: bucket '{bucket}' looks production-like; "
            "pass --allow-production-target to override"
        )
    if postgres_url and is_production_like_postgres_url(postgres_url):
        # The host, never the URL -- a DSN carries a password.
        host = urlparse(postgres_url).hostname or "(unparseable)"
        raise ProductionTargetError(
            f"refusing to seed: Postgres host '{host}' is not a loopback or "
            "private address; pass --allow-production-target to override"
        )
