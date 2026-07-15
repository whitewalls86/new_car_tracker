"""
Plan 120 Gate F — read-only ops/admin snapshot download API.

Exposes existing Plan 120 Gate E archives (already produced by the isolated
`snapshot-worker` / archiver export pipeline) for CI and local dev download.
This router never generates, mutates, or promotes a snapshot — it only reads
the `ci_snapshots/adaptive_refresh/` pointers and the `snapshot_archives/`
manifest/archive objects Gate E already published.

Auth is a standalone bearer token (SNAPSHOT_DOWNLOAD_TOKEN), independent of
the cookie/session admin auth in ops/routers/auth.py — CI callers (and
scripts/download_lake_snapshot.py) have no browser session to present.
"""
import logging
import os
import re
import secrets
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from fastapi.responses import StreamingResponse

from shared.minio import object_size, open_stream, read_json

logger = logging.getLogger("pipeline_ops")

router = APIRouter(prefix="/admin/snapshots/adaptive-refresh", tags=["snapshots"])

ALIAS_PREFIX = "ci_snapshots/adaptive_refresh"
LATEST_KEY = f"{ALIAS_PREFIX}/latest.json"

SNAPSHOT_DOWNLOAD_TOKEN = os.environ.get("SNAPSHOT_DOWNLOAD_TOKEN", "")

# snapshot_id is used to build a MinIO key (aliases/{snapshot_id}.json) — no
# path separators or ".." allowed.
_SNAPSHOT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

# The alias pointer (ci_snapshots/adaptive_refresh/aliases/{snapshot_id}.json)
# is itself an object read from MinIO, not caller input — but its
# archive_manifest_key/archive_key fields are still untrusted content (a
# corrupted or tampered alias object must not turn an authenticated request
# into an arbitrary-object read/stream). Constrain both to the exact known
# Gate E archive prefix before ever passing them to read_json/object_size/
# open_stream.
_ARCHIVE_MANIFEST_KEY_RE = re.compile(
    r"^snapshot_archives/fingerprints/[A-Za-z0-9]{1,128}/archive_manifest\.json$"
)
_ARCHIVE_KEY_RE = re.compile(
    r"^snapshot_archives/fingerprints/[A-Za-z0-9]{1,128}/snapshot\.tar\.zst$"
)


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def require_snapshot_token(authorization: Optional[str] = Header(default=None)) -> None:
    """Bearer-token auth for CI/script callers. Never logs the token itself."""
    if not SNAPSHOT_DOWNLOAD_TOKEN:
        raise HTTPException(status_code=503, detail="snapshot downloads not configured")
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = authorization[len("Bearer "):]
    if not secrets.compare_digest(token, SNAPSHOT_DOWNLOAD_TOKEN):
        raise HTTPException(status_code=403, detail="invalid token")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _alias_key(snapshot_id: str) -> str:
    return f"{ALIAS_PREFIX}/aliases/{snapshot_id}.json"


def _validate_snapshot_id(snapshot_id: str) -> str:
    if not _SNAPSHOT_ID_RE.match(snapshot_id) or ".." in snapshot_id:
        raise HTTPException(status_code=400, detail="invalid snapshot_id")
    return snapshot_id


def _read_json_safe(key: str) -> Optional[Dict[str, Any]]:
    try:
        return read_json(key)
    except Exception:
        logger.warning("snapshot read_json failed key=%s", key, exc_info=True)
        return None


def _resolve_alias(snapshot_id: str) -> Dict[str, Any]:
    alias = _read_json_safe(_alias_key(snapshot_id))
    if not alias:
        raise HTTPException(status_code=404, detail="snapshot not found")
    return alias


def _validated_prefixed_key(value: Any, pattern: "re.Pattern[str]") -> str:
    """Return *value* only if it matches an allowed Gate E object-key shape.

    Treats a non-conforming key (wrong prefix, s3:// URI, absolute path,
    "..", or anything outside snapshot_archives/fingerprints/<id>/...) the
    same as "not found" rather than passing it through to MinIO reads —
    the alias pointer is a stored object, not caller input, but a
    corrupted/tampered one must not be trusted to name an arbitrary key.
    """
    if not isinstance(value, str) or not pattern.match(value):
        logger.warning("snapshot alias referenced an out-of-prefix key: %r", value)
        raise HTTPException(status_code=404, detail="snapshot not found")
    return value


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/latest", dependencies=[Depends(require_snapshot_token)])
def get_latest_snapshot() -> Dict[str, Any]:
    pointer = _read_json_safe(LATEST_KEY)
    if not pointer:
        raise HTTPException(status_code=404, detail="no snapshot has been published yet")
    return pointer


@router.get("/{snapshot_id}", dependencies=[Depends(require_snapshot_token)])
def get_snapshot_manifest(snapshot_id: str) -> Dict[str, Any]:
    snapshot_id = _validate_snapshot_id(snapshot_id)
    alias = _resolve_alias(snapshot_id)

    manifest_key = alias.get("archive_manifest_key")
    if not manifest_key:
        raise HTTPException(status_code=404, detail="snapshot manifest not found")
    manifest_key = _validated_prefixed_key(manifest_key, _ARCHIVE_MANIFEST_KEY_RE)

    manifest = _read_json_safe(manifest_key)
    if not manifest:
        raise HTTPException(status_code=404, detail="snapshot manifest not found")
    return manifest


@router.get("/{snapshot_id}/download", dependencies=[Depends(require_snapshot_token)])
def download_snapshot_archive(snapshot_id: str) -> StreamingResponse:
    snapshot_id = _validate_snapshot_id(snapshot_id)
    alias = _resolve_alias(snapshot_id)

    archive_key = alias.get("archive_key")
    if not archive_key:
        raise HTTPException(status_code=404, detail="snapshot archive not found")
    archive_key = _validated_prefixed_key(archive_key, _ARCHIVE_KEY_RE)

    size = object_size(archive_key)
    if size is None:
        raise HTTPException(status_code=404, detail="snapshot archive not found")

    try:
        chunks = open_stream(archive_key)
    except Exception:
        logger.warning("snapshot archive open_stream failed key=%s", archive_key, exc_info=True)
        raise HTTPException(status_code=404, detail="snapshot archive not found")

    headers = {
        "Content-Disposition": f'attachment; filename="{snapshot_id}.tar.zst"',
        "Content-Length": str(size),
    }
    sha256 = alias.get("archive_sha256")
    if sha256:
        headers["X-Archive-SHA256"] = sha256

    return StreamingResponse(chunks, media_type="application/zstd", headers=headers)
