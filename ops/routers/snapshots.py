"""
Plan 120 Gate F — read-only ops/admin snapshot download API.

Exposes existing Plan 120 Gate E archives (already produced by the isolated
`snapshot-worker` / archiver export pipeline) for CI and local dev download.
This router never generates, mutates, or promotes a snapshot — it only reads
the `ci_snapshots/adaptive_refresh/` pointers and the `snapshot_archives/`
manifest/archive objects Gate E already published.

Auth is a standalone bearer token, independent of the cookie/session admin auth
in ops/routers/auth.py — CI callers (and scripts/download_lake_snapshot.py) have
no browser session to present.

**Tokens are named and scoped**, configured as a set rather than a single
string, because three callers are heading for this route: CI, a developer's
laptop, and the Plan 112 MLflow rehearsal. One shared string gives none of what
having three callers needs — you cannot revoke one without breaking the others,
and the access log cannot say which one it was.

The scope exists ahead of any endpoint that reads it, and deliberately so. Every
entry is `read` today because every route here is read-only. It is in the format
now because the cheapest moment to change a credential format is *before*
anything automated depends on it: once CI holds a token, a format change means
migrating a live caller against a server being modified at the same time. And
the endpoint that would make `write` real is already drafted — Plan 108's
`POST /deploy/trigger` mounts the Docker socket into this container, so a
credential that could reach it is effectively root on the host. A read token
that cannot write is the difference between a leaked download credential and
arbitrary container control.
"""
import logging
import os
import re
import secrets
from typing import Any, Dict, NamedTuple, Optional, Tuple

from fastapi import APIRouter, Depends, Header, HTTPException
from fastapi.responses import StreamingResponse

from shared.minio import object_size, open_stream, read_json

logger = logging.getLogger("pipeline_ops")

router = APIRouter(prefix="/admin/snapshots/adaptive-refresh", tags=["snapshots"])

ALIAS_PREFIX = "ci_snapshots/adaptive_refresh"
LATEST_KEY = f"{ALIAS_PREFIX}/latest.json"

# `name:scope:token` entries, comma separated. The name is for attribution in
# the access log and is never secret; the token never reaches a log line.
#
# Split on the first two colons only, so a token may contain colons — base64 and
# hex tokens do not, but a passphrase might. A comma cannot appear in a token,
# which is the one constraint this format imposes and the reason a malformed
# entry is reported by position rather than silently dropped.
SNAPSHOT_DOWNLOAD_TOKENS = os.environ.get("SNAPSHOT_DOWNLOAD_TOKENS", "")

# The single unnamed token this router accepted before Plan 162. Still honoured,
# so deploying this change breaks nothing and the named set can be introduced by
# a later `.env` edit rather than in lockstep with a container restart. Retire it
# once every caller has a named entry.
SNAPSHOT_DOWNLOAD_TOKEN = os.environ.get("SNAPSHOT_DOWNLOAD_TOKEN", "")

# `write` implies `read`: a caller trusted to mutate is trusted to observe, and
# the alternative — issuing two credentials to one caller — is the arrangement
# people work around rather than follow.
_SCOPE_GRANTS: Dict[str, frozenset] = {
    "read": frozenset({"read"}),
    "write": frozenset({"read", "write"}),
}


class SnapshotToken(NamedTuple):
    name: str
    scope: str
    token: str


def _parse_token_set(raw: str, legacy: str) -> Tuple[SnapshotToken, ...]:
    """Parse `name:scope:token` entries, dropping and reporting malformed ones.

    A bad entry is a warning naming its **position and name**, never its value,
    and never an exception: a typo in one entry must not take the whole router
    down at import and lock every caller out. The caller behind the bad entry
    gets a 403, which is the loud half of the signal.
    """
    entries: list[SnapshotToken] = []
    for position, item in enumerate(raw.split(","), start=1):
        item = item.strip()
        if not item:
            continue
        name, _, rest = item.partition(":")
        scope, separator, token = rest.partition(":")
        if not (name and separator and scope and token):
            logger.warning(
                "snapshot token entry %d is malformed (expected name:scope:token); ignoring",
                position,
            )
            continue
        if scope not in _SCOPE_GRANTS:
            logger.warning(
                "snapshot token entry %d (%s) names unknown scope %r; ignoring",
                position, name, scope,
            )
            continue
        entries.append(SnapshotToken(name=name, scope=scope, token=token))

    if legacy:
        entries.append(SnapshotToken(name="legacy", scope="read", token=legacy))
    return tuple(entries)


SNAPSHOT_TOKENS: Tuple[SnapshotToken, ...] = _parse_token_set(
    SNAPSHOT_DOWNLOAD_TOKENS, SNAPSHOT_DOWNLOAD_TOKEN,
)

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

def _tokens_configured() -> bool:
    """Whether any credential exists at all — a 503, not a 403.

    Separate from :func:`_resolve_token` because the two answer different
    questions and produce different statuses: "this deployment has no tokens"
    is an operator's problem, "your token is wrong" is the caller's.
    """
    return bool(SNAPSHOT_TOKENS)


def _resolve_token(presented: str) -> Optional[SnapshotToken]:
    """Return the entry *presented* matches, or None.

    **This function and the one above are the storage seam.** Everything around
    them — the scope grants, the timing property, the logging, every test — is
    written against the returned entry, not against where it came from. Moving
    credentials out of environment variables and into a table is then one
    function body rather than a change to the auth path.

    The loop compares every entry with no early exit. Breaking on the first
    match leaks nothing about a token's value but does leak which caller
    presented it, through response time.
    """
    matched: Optional[SnapshotToken] = None
    for entry in SNAPSHOT_TOKENS:
        if secrets.compare_digest(presented, entry.token):
            matched = entry
    return matched


def require_snapshot_token(required_scope: str = "read"):
    """Build a dependency asserting the caller holds a token granting *required_scope*.

    A factory rather than a plain dependency so a route declares what it needs —
    `Depends(require_snapshot_token("write"))` — and every read credential is
    refused by construction instead of by somebody remembering to check.

    Never logs a token. Logs the matching entry's **name** on success, which is
    the whole point of naming them: when a scheduled job starts pulling
    snapshots, the access log says which caller it was.
    """
    if required_scope not in _SCOPE_GRANTS:
        raise ValueError(f"unknown scope {required_scope!r}")

    def dependency(authorization: Optional[str] = Header(default=None)) -> None:
        if not _tokens_configured():
            raise HTTPException(
                status_code=503, detail="snapshot downloads not configured",
            )
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="missing bearer token")

        matched = _resolve_token(authorization[len("Bearer "):])
        if matched is None:
            raise HTTPException(status_code=403, detail="invalid token")

        if required_scope not in _SCOPE_GRANTS[matched.scope]:
            # Names the scopes, never the token. A caller debugging a 403 needs
            # to know it authenticated and was refused on authority, which is a
            # different fix from a wrong token.
            logger.warning(
                "snapshot token caller=%s scope=%s refused; route requires %s",
                matched.name, matched.scope, required_scope,
            )
            raise HTTPException(
                status_code=403,
                detail=(
                    f"token has scope '{matched.scope}', "
                    f"this route requires '{required_scope}'"
                ),
            )

        logger.info(
            "snapshot request authorized caller=%s scope=%s required=%s",
            matched.name, matched.scope, required_scope,
        )

    return dependency


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
    if alias.get("snapshot_id") != snapshot_id:
        # A corrupted/mismatched alias object must never silently serve a
        # different snapshot's manifest/archive under this snapshot_id's URL.
        logger.warning(
            "snapshot alias snapshot_id mismatch: requested=%s alias_snapshot_id=%r",
            snapshot_id, alias.get("snapshot_id"),
        )
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


def _manifest_for_alias(
    snapshot_id: str, alias: Dict[str, Any], manifest: Dict[str, Any],
) -> Dict[str, Any]:
    """Return the archive manifest adapted to the requested snapshot id.

    Gate E archives are keyed by export_fingerprint and can be reused by
    multiple snapshot ids. That means the persisted archive_manifest.json may
    legitimately carry the original snapshot_id from the first packaging run.
    The per-snapshot alias is the identity pointer for Gate F URLs, so we
    enforce archive consistency against the alias and overlay the requested
    snapshot_id before returning the manifest to download clients.
    """
    archive = manifest.get("archive")
    if not isinstance(archive, dict):
        logger.warning("snapshot manifest missing archive block: snapshot_id=%s", snapshot_id)
        raise HTTPException(status_code=404, detail="snapshot manifest not found")

    expected = {
        "path": alias.get("archive_key"),
        "bytes": alias.get("archive_bytes"),
        "sha256": alias.get("archive_sha256"),
    }
    actual = {
        "path": archive.get("path"),
        "bytes": archive.get("bytes"),
        "sha256": archive.get("sha256"),
    }
    if actual != expected:
        logger.warning(
            "snapshot manifest archive mismatch: snapshot_id=%s expected=%r actual=%r",
            snapshot_id, expected, actual,
        )
        raise HTTPException(status_code=404, detail="snapshot manifest not found")

    response = dict(manifest)
    response["snapshot_id"] = snapshot_id
    return response


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/latest", dependencies=[Depends(require_snapshot_token("read"))])
def get_latest_snapshot() -> Dict[str, Any]:
    pointer = _read_json_safe(LATEST_KEY)
    if not pointer:
        raise HTTPException(status_code=404, detail="no snapshot has been published yet")
    return pointer


@router.get("/{snapshot_id}", dependencies=[Depends(require_snapshot_token("read"))])
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
    return _manifest_for_alias(snapshot_id, alias, manifest)


@router.get("/{snapshot_id}/download", dependencies=[Depends(require_snapshot_token("read"))])
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
