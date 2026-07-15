"""
Snapshot archive packaging/upload for CI lake snapshot exports (Plan 120
Gate E).

Takes a successfully materialized export (Gate D: a fingerprint-addressed
filtered Parquet dataset plus its `manifest.json`) and packages it into a
single `snapshot.tar.zst` archive, uploads it to MinIO under
`snapshot_archives/fingerprints/{export_fingerprint}/`, and — only after that
succeeds — promotes the friendly `latest.json`/`aliases/{snapshot_id}.json`
pointers CI/local downloaders read.

Mirrors the dual-mode pattern already used by `lake_snapshot_export.py`: when
`base_path` is set, data is read/written on the local filesystem (fast,
MinIO-free unit/integration tests); when it is None, reads go through
`shared.minio.get_s3fs()` and writes go through `shared.minio`'s
boto3-backed helpers (production/VM path, and integration tests that mock
boto3/s3fs).

The archive is keyed by `export_fingerprint` directly — packaging is a pure
function of the materialized export, so no separate "archive fingerprint" is
needed. Because there is exactly one canonical archive object per
fingerprint (unlike Gate D's uniquely-named generation directories), this
module never blindly overwrites an existing valid archive: a freshly built
archive whose sha256 matches what's already published is treated as a no-op
reuse; a freshly built archive that differs from an existing valid one is
refused unless the caller explicitly asked for `refresh_archive_cache`.
"""
import hashlib
import io
import json
import logging
import os
import tarfile
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import zstandard

from shared.minio import (
    BUCKET,
    get_s3fs,
    object_size,
    read_bytes,
    read_json,
    write_bytes,
    write_json,
)

logger = logging.getLogger("archiver")

ARCHIVE_CACHE_SCHEMA_VERSION = 1
DEFAULT_ARCHIVE_PREFIX = "snapshot_archives"
DEFAULT_ALIAS_PREFIX = "ci_snapshots/adaptive_refresh"
ARCHIVE_OBJECT_NAME = "snapshot.tar.zst"
ARCHIVE_MANIFEST_OBJECT_NAME = "archive_manifest.json"

# Same level used for other zstd writes in this codebase (shared.minio.ZSTD_LEVEL);
# duplicated as a literal so this module's archive bytes stay stable even if
# that constant's tuning changes for the unrelated HTML-compression path.
_ZSTD_LEVEL = 9


class LakeSnapshotArchiveError(Exception):
    """Raised on an unsafe archive member or an unresolvable archive conflict."""


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def archive_manifest_path(prefix: str, fingerprint: str) -> str:
    return f"{prefix.rstrip('/')}/fingerprints/{fingerprint}/{ARCHIVE_MANIFEST_OBJECT_NAME}"


def archive_object_path(prefix: str, fingerprint: str) -> str:
    return f"{prefix.rstrip('/')}/fingerprints/{fingerprint}/{ARCHIVE_OBJECT_NAME}"


def latest_pointer_path(alias_prefix: str) -> str:
    return f"{alias_prefix.rstrip('/')}/latest.json"


def alias_pointer_path(alias_prefix: str, snapshot_id: str) -> str:
    return f"{alias_prefix.rstrip('/')}/aliases/{snapshot_id}.json"


# ---------------------------------------------------------------------------
# Dual-mode (local base_path vs MinIO) object I/O
# ---------------------------------------------------------------------------

def _local_root(base_path: str, relative: str) -> str:
    return os.path.join(base_path.rstrip("/"), relative)


def _read_json_object(base_path: Optional[str], key: str) -> Optional[Dict[str, Any]]:
    if base_path:
        full = _local_root(base_path, key)
        if not os.path.exists(full):
            return None
        with open(full, "r", encoding="utf-8") as f:
            return json.load(f)
    try:
        return read_json(key)
    except Exception as e:
        logger.warning("lake_snapshot_archive: read_json failed key=%s error=%s", key, e)
        return None


def _write_json_object(base_path: Optional[str], key: str, obj: Dict[str, Any]) -> bool:
    try:
        if base_path:
            full = _local_root(base_path, key)
            os.makedirs(os.path.dirname(full), exist_ok=True)
            with open(full, "w", encoding="utf-8") as f:
                json.dump(obj, f, sort_keys=True, indent=2)
        else:
            write_json(key, obj)
        return True
    except Exception as e:
        logger.warning("lake_snapshot_archive: write_json failed key=%s error=%s", key, e)
        return False


def _write_bytes_object(base_path: Optional[str], key: str, data: bytes) -> None:
    if base_path:
        full = _local_root(base_path, key)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "wb") as f:
            f.write(data)
    else:
        write_bytes(key, data, content_type="application/zstd")


def _object_byte_size(base_path: Optional[str], key: str) -> Optional[int]:
    if base_path:
        full = _local_root(base_path, key)
        return os.path.getsize(full) if os.path.exists(full) else None
    return object_size(key)


def _read_bytes_object(base_path: Optional[str], key: str) -> bytes:
    if base_path:
        with open(_local_root(base_path, key), "rb") as f:
            return f.read()
    return read_bytes(key)


# ---------------------------------------------------------------------------
# Safe, deterministic file listing under a materialized data_path
# ---------------------------------------------------------------------------

def _is_unsafe_relative_path(rel: str) -> bool:
    if not rel or rel.startswith("/") or rel.startswith(".."):
        return True
    return any(part in ("", "..") for part in rel.split("/"))


def list_data_files(base_path: Optional[str], data_path: str) -> List[str]:
    """Return a sorted, archive-relative file list under a materialized
    export's `data_path`. Never follows symlinks and refuses any entry whose
    relative path would traverse outside `data_path`."""
    results: List[str] = []
    if base_path:
        root = _local_root(base_path, data_path)
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [
                d for d in dirnames if not os.path.islink(os.path.join(dirpath, d))
            ]
            for name in filenames:
                full = os.path.join(dirpath, name)
                if os.path.islink(full):
                    logger.warning("lake_snapshot_archive: skipping symlink %s", full)
                    continue
                rel = os.path.relpath(full, root).replace(os.sep, "/")
                if _is_unsafe_relative_path(rel):
                    raise LakeSnapshotArchiveError(f"unsafe relative path under {root}: {rel}")
                results.append(rel)
    else:
        fs = get_s3fs()
        root = f"{BUCKET}/{data_path}".rstrip("/")
        for full in fs.find(root):
            rel = full[len(root):].lstrip("/")
            if _is_unsafe_relative_path(rel):
                raise LakeSnapshotArchiveError(f"unsafe relative path under {root}: {rel}")
            results.append(rel)
    return sorted(results)


def _read_data_file(base_path: Optional[str], data_path: str, rel: str) -> bytes:
    if base_path:
        with open(_local_root(base_path, f"{data_path}/{rel}"), "rb") as f:
            return f.read()
    fs = get_s3fs()
    with fs.open(f"{BUCKET}/{data_path}/{rel}", "rb") as f:
        return f.read()


# ---------------------------------------------------------------------------
# tar.zst construction
# ---------------------------------------------------------------------------

def _add_bytes_to_tar(tar: tarfile.TarFile, name: str, data: bytes) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(data)
    info.mtime = 0
    info.uid = info.gid = 0
    info.uname = info.gname = ""
    info.mode = 0o644
    tar.addfile(info, io.BytesIO(data))


def build_archive_tar_zst(
    base_path: Optional[str],
    data_path: str,
    export_manifest: Dict[str, Any],
    relative_paths: List[str],
    dest_path: str,
) -> Tuple[str, int]:
    """Write a deterministic `.tar.zst` to `dest_path` (a local filesystem
    path) containing `export_manifest` as `manifest.json` plus every file in
    `relative_paths` under the materialized `data_path`, in the given
    (already-sorted) order. Returns (sha256, byte_count) of the written file.

    Determinism: every archive member gets fixed mtime/uid/gid/mode, members
    are added in the caller-supplied (sorted) order, and the manifest is
    serialized with sorted keys — so identical inputs always produce
    identical archive bytes.
    """
    manifest_bytes = json.dumps(export_manifest, sort_keys=True, indent=2).encode("utf-8")
    cctx = zstandard.ZstdCompressor(level=_ZSTD_LEVEL)
    with open(dest_path, "wb") as raw_fh:
        with cctx.stream_writer(raw_fh) as compressor:
            with tarfile.open(fileobj=compressor, mode="w|") as tar:
                _add_bytes_to_tar(tar, "manifest.json", manifest_bytes)
                for rel in relative_paths:
                    data = _read_data_file(base_path, data_path, rel)
                    _add_bytes_to_tar(tar, rel, data)

    digest = hashlib.sha256()
    size = 0
    with open(dest_path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


# ---------------------------------------------------------------------------
# Archive manifest (the external, checksum-bearing manifest CI/local
# downloaders load — see scripts/lake_snapshot_common.py:get_archive_meta)
# ---------------------------------------------------------------------------

def build_archive_manifest(
    export_manifest: Dict[str, Any],
    *,
    archive_key: str,
    archive_bytes: int,
    archive_sha256: str,
    file_count: int,
) -> Dict[str, Any]:
    """Build the published archive manifest: the Gate D export manifest plus
    archive metadata. This is the manifest CI/local downloaders read — the
    in-archive `manifest.json` intentionally omits `archive` (its own
    checksum can't be known until after it is itself packaged)."""
    manifest = dict(export_manifest)
    manifest["archive_cache_schema_version"] = ARCHIVE_CACHE_SCHEMA_VERSION
    manifest["archive"] = {
        "path": archive_key,
        "bytes": archive_bytes,
        "sha256": archive_sha256,
        "file_count": file_count,
    }
    manifest["archived_at"] = datetime.now(timezone.utc).isoformat()
    return manifest


def _archive_manifest_incompleteness_reason(
    manifest: Dict[str, Any], expected_fingerprint: str,
) -> Optional[str]:
    if manifest.get("archive_cache_schema_version") != ARCHIVE_CACHE_SCHEMA_VERSION:
        return (
            f"archive_cache_schema_version mismatch: "
            f"{manifest.get('archive_cache_schema_version')!r}"
        )
    if manifest.get("export_fingerprint") != expected_fingerprint:
        return (
            f"export_fingerprint mismatch: manifest has "
            f"{manifest.get('export_fingerprint')!r}, expected {expected_fingerprint!r}"
        )
    archive = manifest.get("archive") or {}
    if not archive.get("sha256") or not archive.get("bytes") or not archive.get("path"):
        return "incomplete archive metadata"
    return None


def _object_sha256(base_path: Optional[str], key: str) -> Optional[str]:
    """Return the actual sha256 of an object's bytes, or None if it can't be
    read (missing object, transient error, etc.)."""
    try:
        data = _read_bytes_object(base_path, key)
    except Exception as e:
        logger.warning("lake_snapshot_archive: failed to read object key=%s error=%s", key, e)
        return None
    return hashlib.sha256(data).hexdigest()


def load_archive_manifest(
    base_path: Optional[str], manifest_key: str, expected_fingerprint: str,
) -> Optional[Dict[str, Any]]:
    """Load a previously published archive manifest, verified against the
    actual archive object: schema/fingerprint/field-presence from the
    manifest JSON itself, a cheap size check (fast-fails an obviously
    truncated/corrupt object without downloading it), and then the archive
    object's real sha256 — a same-size-but-corrupted object must never be
    silently trusted as a valid cache hit just because the manifest JSON
    still looks complete. Returns None on any miss or validation failure
    (never raises)."""
    manifest = _read_json_object(base_path, manifest_key)
    if manifest is None:
        return None
    reason = _archive_manifest_incompleteness_reason(manifest, expected_fingerprint)
    if reason is not None:
        logger.warning(
            "lake_snapshot_archive: manifest invalid key=%s reason=%s; treating as miss",
            manifest_key, reason,
        )
        return None
    archive = manifest["archive"]
    actual_size = _object_byte_size(base_path, archive["path"])
    if actual_size is None:
        logger.warning(
            "lake_snapshot_archive: archive object missing key=%s manifest_key=%s; "
            "treating as miss", archive["path"], manifest_key,
        )
        return None
    if actual_size != archive["bytes"]:
        logger.warning(
            "lake_snapshot_archive: archive size mismatch key=%s manifest_bytes=%s "
            "actual_bytes=%s; treating as miss",
            archive["path"], archive["bytes"], actual_size,
        )
        return None
    actual_sha256 = _object_sha256(base_path, archive["path"])
    if actual_sha256 != archive["sha256"]:
        logger.warning(
            "lake_snapshot_archive: archive checksum mismatch key=%s manifest_sha256=%s "
            "actual_sha256=%s; treating as miss",
            archive["path"], archive["sha256"], actual_sha256,
        )
        return None
    return manifest


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

@dataclass
class ArchiveResult:
    ok: bool
    archive_key: Optional[str] = None
    archive_manifest_key: Optional[str] = None
    archive_bytes: Optional[int] = None
    archive_sha256: Optional[str] = None
    file_count: Optional[int] = None
    cache_hit: bool = False
    cache_action: Optional[str] = None
    archive_manifest: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


def package_snapshot_archive(
    base_path: Optional[str],
    data_path: str,
    export_manifest: Dict[str, Any],
    export_fingerprint: str,
    archive_prefix: str = DEFAULT_ARCHIVE_PREFIX,
    reuse_archive_cache: bool = False,
    refresh_archive_cache: bool = False,
) -> ArchiveResult:
    """Package a materialized export into `snapshot.tar.zst` and publish it
    (plus its archive manifest) under `{archive_prefix}/fingerprints/{export_fingerprint}/`.

    Never overwrites a previously published, still-valid archive unless
    `refresh_archive_cache=True`: a freshly built archive whose sha256 matches
    the existing one is a harmless no-op ("reused"); one that differs is a
    hard conflict (fails rather than silently clobbering good data) unless
    refresh was explicitly requested.
    """
    archive_manifest_key = archive_manifest_path(archive_prefix, export_fingerprint)
    archive_key = archive_object_path(archive_prefix, export_fingerprint)

    if reuse_archive_cache and not refresh_archive_cache:
        cached = load_archive_manifest(base_path, archive_manifest_key, export_fingerprint)
        if cached is not None:
            logger.info(
                "lake_snapshot_archive: cache hit export_fingerprint=%s key=%s",
                export_fingerprint, archive_manifest_key,
            )
            archive = cached["archive"]
            return ArchiveResult(
                ok=True,
                archive_key=archive["path"],
                archive_manifest_key=archive_manifest_key,
                archive_bytes=archive["bytes"],
                archive_sha256=archive["sha256"],
                file_count=archive.get("file_count"),
                cache_hit=True,
                cache_action="reused",
                archive_manifest=cached,
            )

    relative_paths = list_data_files(base_path, data_path)
    t0 = time.monotonic()
    logger.info(
        "lake_snapshot_archive: package start export_fingerprint=%s files=%d",
        export_fingerprint, len(relative_paths),
    )
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".tar.zst", prefix="lake-snapshot-")
    os.close(tmp_fd)
    try:
        sha256, size = build_archive_tar_zst(
            base_path, data_path, export_manifest, relative_paths, tmp_path,
        )
        logger.info(
            "lake_snapshot_archive: package end elapsed_s=%.2f export_fingerprint=%s "
            "bytes=%d sha256=%s",
            time.monotonic() - t0, export_fingerprint, size, sha256,
        )

        # Always check for a previously published archive (even when
        # refreshing) so the reported cache_action ("computed" vs
        # "refreshed") reflects whether one actually existed.
        existing = load_archive_manifest(base_path, archive_manifest_key, export_fingerprint)

        if existing is not None and not refresh_archive_cache:
            existing_archive = existing["archive"]
            if existing_archive["sha256"] == sha256:
                logger.info(
                    "lake_snapshot_archive: identical archive already published "
                    "export_fingerprint=%s key=%s; skipping re-upload",
                    export_fingerprint, archive_manifest_key,
                )
                return ArchiveResult(
                    ok=True,
                    archive_key=existing_archive["path"],
                    archive_manifest_key=archive_manifest_key,
                    archive_bytes=existing_archive["bytes"],
                    archive_sha256=existing_archive["sha256"],
                    file_count=existing_archive.get("file_count"),
                    cache_hit=True,
                    cache_action="reused",
                    archive_manifest=existing,
                )
            return ArchiveResult(
                ok=False,
                archive_manifest_key=archive_manifest_key,
                error=(
                    f"refusing to overwrite existing archive for "
                    f"export_fingerprint={export_fingerprint}: recorded sha256 "
                    f"{existing_archive['sha256']!r} != freshly built {sha256!r}; "
                    "pass refresh_archive_cache=True to force"
                ),
            )

        with open(tmp_path, "rb") as f:
            data = f.read()
        _write_bytes_object(base_path, archive_key, data)

        archive_manifest = build_archive_manifest(
            export_manifest,
            archive_key=archive_key,
            archive_bytes=size,
            archive_sha256=sha256,
            file_count=len(relative_paths),
        )
        written = _write_json_object(base_path, archive_manifest_key, archive_manifest)
        if not written:
            return ArchiveResult(
                ok=False,
                archive_key=archive_key,
                archive_manifest_key=archive_manifest_key,
                archive_bytes=size,
                archive_sha256=sha256,
                file_count=len(relative_paths),
                error=f"archive manifest write failed: {archive_manifest_key}",
            )

        action = "refreshed" if (refresh_archive_cache and existing is not None) else "computed"
        return ArchiveResult(
            ok=True,
            archive_key=archive_key,
            archive_manifest_key=archive_manifest_key,
            archive_bytes=size,
            archive_sha256=sha256,
            file_count=len(relative_paths),
            cache_hit=False,
            cache_action=action,
            archive_manifest=archive_manifest,
        )
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def promote_snapshot_pointers(
    base_path: Optional[str],
    snapshot_id: str,
    export_fingerprint: str,
    archive_result: ArchiveResult,
    alias_prefix: str = DEFAULT_ALIAS_PREFIX,
) -> Dict[str, Any]:
    """Write `aliases/{snapshot_id}.json` then `latest.json`, in that order —
    `latest.json` is the last thing any caller should promote, so a failure
    partway through never leaves `latest.json` pointing at a snapshot whose
    alias file doesn't exist. Must only be called after a successful
    (`archive_result.ok`) package/upload; callers are responsible for that
    ordering.

    Returns `{"ok": bool, "alias_key", "latest_key", "error"}`. Never raises
    on a pointer-write failure — the caller must check `ok` and treat a
    failure as the export not being fully published (only the archive
    itself and its own manifest were), since a caller that ignored this
    would otherwise report "exported" while the friendly `latest.json`/alias
    pointers silently failed to move.
    """
    if not archive_result.ok:
        raise LakeSnapshotArchiveError(
            "promote_snapshot_pointers called with a failed archive_result"
        )

    pointer = {
        "snapshot_id": snapshot_id,
        "export_fingerprint": export_fingerprint,
        "archive_key": archive_result.archive_key,
        "archive_manifest_key": archive_result.archive_manifest_key,
        "archive_bytes": archive_result.archive_bytes,
        "archive_sha256": archive_result.archive_sha256,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    alias_key = alias_pointer_path(alias_prefix, snapshot_id)
    if not _write_json_object(base_path, alias_key, pointer):
        return {
            "ok": False, "alias_key": alias_key, "latest_key": None,
            "error": f"alias pointer write failed: {alias_key}",
        }

    latest_key = latest_pointer_path(alias_prefix)
    if not _write_json_object(base_path, latest_key, pointer):
        return {
            "ok": False, "alias_key": alias_key, "latest_key": latest_key,
            "error": f"latest pointer write failed: {latest_key}",
        }

    return {"ok": True, "alias_key": alias_key, "latest_key": latest_key, "error": None}
