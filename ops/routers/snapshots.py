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

**Tokens are named, scoped, and live in `ops.machine_tokens`** — one row per
caller, because three callers are heading for this route: CI, a developer's
laptop, and the Plan 112 MLflow rehearsal. One shared string gives none of what
having three callers needs — you cannot revoke one without breaking the others,
and the access log cannot say which one it was.

**A row carries what an environment variable cannot: an expiry, a revocation
and a last-used timestamp.** Revocation is not the hard part — an `UPDATE` is
trivial, and it takes effect on the next request with no restart. Standing in
front of a credential in six months and deciding whether anything still uses it
is the hard part, and `last_used_at` is the only thing here that can answer it.
Only a digest is stored, so a `pg_dump`, a screenshot of pgAdmin or a stray
query result hands over nothing usable. Plan 173.

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
import hashlib
import logging
import re
from typing import Any, Dict, NamedTuple, Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from fastapi.responses import StreamingResponse

from ops.queries import (
    SELECT_ACTIVE_MACHINE_TOKEN_EXISTS,
    SELECT_MACHINE_TOKEN,
    TOUCH_MACHINE_TOKEN_LAST_USED,
)
from shared.db import db_cursor
from shared.db_vocabularies import MachineTokenScope
from shared.minio import object_size, open_stream, read_json

logger = logging.getLogger("pipeline_ops")

router = APIRouter(prefix="/admin/snapshots/adaptive-refresh", tags=["snapshots"])

ALIAS_PREFIX = "ci_snapshots/adaptive_refresh"
LATEST_KEY = f"{ALIAS_PREFIX}/latest.json"

# How stale `last_used_at` may get before a request refreshes it. Bound as a
# parameter into touch_machine_token_last_used.sql, which names no window of its
# own, so there is one place to change this.
#
# The column exists to answer "is anything still using this credential" before
# somebody revokes it. That question tolerates being minutes stale and does not
# justify a write per download.
LAST_USED_THROTTLE = "5 minutes"

# `write` implies `read`: a caller trusted to mutate is trusted to observe, and
# the alternative — issuing two credentials to one caller — is the arrangement
# people work around rather than follow.
#
# The two scope *values* are the database's, and read from there rather than
# retyped: `ops.machine_tokens.scope` carries a CHECK, so a third scope arrives
# by migration and this table is where it has to be granted. What stays here is
# the implication, which is policy the column cannot express.
_SCOPE_GRANTS: Dict[str, frozenset] = {
    MachineTokenScope.READ: frozenset({MachineTokenScope.READ}),
    MachineTokenScope.WRITE: frozenset({MachineTokenScope.READ, MachineTokenScope.WRITE}),
}


class SnapshotToken(NamedTuple):
    """An authenticated caller — who it is and what it may do.

    Carries no token. It once did, back when the credential *was* the string in
    the environment, and dropping the field is what makes the storage swap
    honest: the auth path above :func:`_resolve_token` reads ``name`` and
    ``scope`` and never had a use for the third field. A credential kept on the
    object every route handler receives is one a later change can log by
    accident.
    """
    name: str
    scope: str


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

def token_digest(presented: str) -> str:
    """The stored form of a token: SHA-256, hex.

    Public because :mod:`scripts.issue_machine_token` computes the digest it
    inserts, and this is the only definition of what "the digest" means. Two
    copies that disagree would mean no issued token ever authenticates — an
    import is the cheaper failure mode than a duplicated one-liner.
    """
    return hashlib.sha256(presented.encode("utf-8")).hexdigest()


def _tokens_configured() -> bool:
    """Whether any credential exists at all — a 503, not a 403.

    Separate from :func:`_resolve_token` because the two answer different
    questions and produce different statuses: "this deployment has no tokens"
    is an operator's problem, "your token is wrong" is the caller's.

    Answered from the table alone since Plan 173's second deploy retired the
    environment set. A deployment whose only credential expired overnight
    reports itself unconfigured, which is true, rather than refusing every
    caller as though each had presented a bad token.
    """
    return _machine_tokens_exist()


def _machine_tokens_exist() -> bool:
    """Whether `ops.machine_tokens` holds a credential that is still usable.

    An unreadable table answers False rather than raising, which lands the
    caller on :func:`_tokens_configured`'s 503. A database this route cannot
    reach is an operator's problem too, and it is emphatically not the caller's
    token being wrong.
    """
    try:
        with db_cursor(error_context="Machine token configuration check") as cur:
            cur.execute(SELECT_ACTIVE_MACHINE_TOKEN_EXISTS)
            row = cur.fetchone()
    except Exception:
        logger.warning("machine token configuration check failed", exc_info=True)
        return False
    return bool(row and row[0])


def _resolve_token(presented: str) -> Optional[SnapshotToken]:
    """Return the caller *presented* authenticates as, or None.

    **This function and the one above are the storage seam.** Everything around
    them — the scope grants, the logging, every test — is written against the
    returned entry, not against where it came from, which is what made moving
    credentials out of environment variables into a table two function bodies
    rather than a change to the auth path.

    The table is the only source. It briefly shared the job with a
    `name:scope:token` environment variable, for one deploy, because the first
    table-backed token could not be issued through an interface that already
    required one — that set was retired on 2026-09-08 once every caller held a
    row.
    """
    return _resolve_machine_token(presented)


def _resolve_machine_token(presented: str) -> Optional[SnapshotToken]:
    """Look *presented* up in `ops.machine_tokens`, honouring its lifecycle.

    One indexed probe on the digest, with no per-entry comparison — which is
    what hashing bought over the environment set's deliberate full scan, rather
    than something reimplemented on top of it.

    A revoked or expired row is refused and **says which in the log**, while the
    caller gets the same 403 as an unknown token. The distinction is for whoever
    reads the log later; the response deliberately does not carry it.
    """
    digest = token_digest(presented)
    try:
        with db_cursor(error_context="Machine token lookup", dict_cursor=True) as cur:
            cur.execute(SELECT_MACHINE_TOKEN, (digest,))
            row = cur.fetchone()
    except Exception:
        # Falls through to the environment set, and to a 403 once that is gone.
        # Never a 500: the database being unreachable is already reported as a
        # 503 by _tokens_configured, which runs first.
        logger.warning("machine token lookup failed", exc_info=True)
        return None

    if row is None:
        return None
    if row["is_revoked"]:
        logger.warning("machine token caller=%s refused: revoked", row["name"])
        return None
    if row["is_expired"]:
        logger.warning("machine token caller=%s refused: expired", row["name"])
        return None

    # Inlined rather than delegated, so the guards above and this write sit in
    # one frame. Plan 162 Stage Y: the write is correct only because `row is
    # None` and the two lifecycle refusals return before it, and while it lived
    # in a helper nothing connected the two -- a second caller would have got a
    # write with no guard, and neither half's tests would have noticed. That is
    # the seam Plan 158's defect lived in.
    #
    # A failure here is logged and swallowed. A credential that has already
    # authenticated must not be refused because its bookkeeping write failed --
    # the cost of that is a stale answer to "is anything still using this",
    # which is strictly better than a download that 500s. Nothing about the
    # response depends on it, which is why the swallow changes nothing the
    # caller can observe.
    #
    # `rowcount` is deliberately not read: the throttle is in the statement's
    # WHERE, so zero rows is the designed outcome whenever the window has not
    # elapsed, and a check on it would fire on nearly every authenticated call.
    try:
        with db_cursor(error_context="Machine token last-used") as cur:
            cur.execute(TOUCH_MACHINE_TOKEN_LAST_USED, (digest, LAST_USED_THROTTLE))
    except Exception:
        logger.warning("machine token last_used_at update failed", exc_info=True)

    return SnapshotToken(name=row["name"], scope=row["scope"])


def require_snapshot_token(required_scope: str = MachineTokenScope.READ):
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

@router.get(
    "/latest",
    dependencies=[Depends(require_snapshot_token("read"))],
    responses={
        401: {"description": "No usable machine credential was presented."},
        403: {"description": "The credential does not grant the scope this route needs."},
        404: {"description": "No snapshot with that id."},
        503: {"description": "Database unavailable."},
    },
)
def get_latest_snapshot() -> Dict[str, Any]:
    pointer = _read_json_safe(LATEST_KEY)
    if not pointer:
        raise HTTPException(status_code=404, detail="no snapshot has been published yet")
    return pointer


@router.get(
    "/{snapshot_id}",
    dependencies=[Depends(require_snapshot_token("read"))],
    responses={
        400: {"description": "The snapshot id is not a well-formed identifier."},
        401: {"description": "No usable machine credential was presented."},
        403: {"description": "The credential does not grant the scope this route needs."},
        404: {"description": "No snapshot with that id."},
        503: {"description": "Database unavailable."},
    },
)
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


@router.get(
    "/{snapshot_id}/download",
    response_class=StreamingResponse,
    dependencies=[Depends(require_snapshot_token("read"))],
    responses={
        400: {"description": "The snapshot id is not a well-formed identifier."},
        401: {"description": "No usable machine credential was presented."},
        403: {"description": "The credential does not grant the scope this route needs."},
        404: {"description": "No snapshot with that id."},
        503: {"description": "Database unavailable."},
    },
)
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
