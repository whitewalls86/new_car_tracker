"""Plan 131 Stage 4: delete bronze HTML objects that are provably inside a pack.

**This is the only step in Plan 131 that removes data**, and the only one that
frees an inode. Stages 1-3 wrote packs alongside their sources and changed the
read path; nothing has freed a single inode yet. At ~65,500 inodes/day against
~4.0M free the ceiling is around mid-October 2026, and April alone is ~1.248M
inodes (~19 days), April+May+June ~6.05M (+92 days).

The safety property, stated exactly:

    An object is deleted only after the pack that replaced it has returned its
    exact bytes, for that artifact, verified against the sidecar's own
    raw_sha256 AND against the live object about to be deleted.

Everything else this job does is bookkeeping around that sentence.

Three checks per member, none of them optional:

* **Resolvable.** ``pack_lookup_prefix(source_key)`` must name the prefix this
  pack actually lives under. If it does not, a reader could never find the
  member after its object is gone, and it is refused — bytes that exist but
  cannot be located are not a replacement for anything.
* **Extractable and self-consistent.** The member is extracted from the
  *stored* pack over ranged GETs, through the same ``PackReader`` the
  production read path uses, and its sha256 must equal the sidecar's
  ``raw_sha256``.
* **Identical to the object being deleted.** The source object is read and
  decompressed, and its bytes must equal the extracted bytes. This is the check
  that makes the deletion safe rather than merely consistent: a pack that
  agrees with its own index but not with the object would otherwise pass.

Per *pack*, a bounded sample additionally goes through ``read_packed_html`` end
to end, so the real resolver — prefix derivation, sidecar listing, index lookup
— is exercised against production data rather than assumed from the per-member
checks. Doing that for every member would rescan every earlier sidecar in the
month for every artifact; doing it for none would leave resolution untested on
the one path that has to work afterwards.

What this job will not do:

* delete anything with no sidecar entry;
* delete from an **orphan pack** (a pack with no sidecar — an interrupted run).
  Stage 2 reports these and never deletes them; that property is kept here;
* treat any processing status as junk. ``ok`` is the n8n-era **success** status,
  19,950 April artifacts carry it, and no month since has any;
* refuse an artifact for having no provenance. 42,276 April captures have no
  ``artifacts_queue_events`` row at all and can never satisfy a status
  predicate; they are inside verified packs, so they are deleted like any other
  member and counted under ``no_event_row``.

**Processing status is reported, never a veto** (plan doc, agreed 2026-08-14).
The check existed so an unparsed artifact would not be deleted before it could
be parsed; Stage 3 makes a packed artifact readable and parseable after its
object is gone, so the check stopped being load-bearing. It is still measured,
because a run that suddenly deletes tens of thousands of ``pending`` objects is
worth seeing.

**The delete grace period defaults to 0 days.** A 14-day default was proposed
and did not survive being asked what it protects against — see the plan doc.
The knob remains for the case where something does surface.
"""
from __future__ import annotations

import argparse
import calendar
import hashlib
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from shared.minio import BUCKET, get_boto3_client
from shared.packfile import (
    PackIndexEntry,
    PackReader,
    index_key,
    read_index_parquet,
)

logger = logging.getLogger("archiver")

#: Patched to a throwaway namespace by the integration tests, exactly as the
#: packer's are, so a test run cannot touch production objects.
_HTML_PREFIX = "html"
_PACK_PREFIX = "html_packs"
_ARTIFACT_EVENTS_PATH = "s3://{bucket}/ops_normalized/artifacts_queue_events/**/*.parquet"

#: Objects deleted per run. Small on purpose: the first runs are meant to be
#: inspected by hand, and the bucket is un-versioned, so a delete is immediate
#: and there is no undo.
MAX_OBJECTS = int(os.environ.get("PACK_PRUNE_MAX_OBJECTS", "1000"))

#: Packs drained per run. One pack's worth of sources at a time — deletes are
#: never batched across packs.
MAX_PACKS = int(os.environ.get("PACK_PRUNE_MAX_PACKS", "1"))

#: Days a pack must have existed before its sources may be deleted, measured
#: from the **sidecar's** write time — the moment the pack became referenceable,
#: and a timestamp that means what it says (unlike a source object's
#: LastModified, which Plan 129's backfill reset across the whole corpus).
#:
#: **Zero by default, deliberately.** Neither argument for a waiting period
#: survived examination: pack loss is not concentrated in the days after
#: writing, and "time for a human to notice" names no mechanism that does the
#: noticing. Per-artifact verification at delete time is what makes this safe.
#: The knob stays because it costs nothing and is the lever if that is ever
#: shown wrong.
DELETE_GRACE_DAYS = int(os.environ.get("PACK_DELETE_GRACE_DAYS", "0"))

#: Members per pack additionally read through the full production resolver.
SAMPLE_FULL_READS = int(os.environ.get("PACK_PRUNE_SAMPLE_FULL_READS", "25"))

#: Inodes per object on this filesystem: one directory plus one xl.meta,
#: measured at ~2.24 across the bucket (Plan 131 Stage 0a). Used only to
#: estimate; the measured free-inode delta is reported alongside it.
INODES_PER_OBJECT = float(os.environ.get("PACK_PRUNE_INODES_PER_OBJECT", "2.24"))

DUCKDB_MEMORY_LIMIT = os.environ.get("PACK_PRUNE_DUCKDB_MEMORY", "2GB")
DUCKDB_THREADS = int(os.environ.get("PACK_PRUNE_DUCKDB_THREADS", "2"))

PROGRESS_EVERY = int(os.environ.get("PACK_PRUNE_PROGRESS_EVERY", "1000"))

DEFAULT_ARTIFACT_TYPE = "detail_page"

#: Reported statuses. `ok` is success (n8n era) and is never junk; `no_event_row`
#: is the "packed and verified, no provenance" class.
NO_EVENT_ROW = "no_event_row"


class PruneVerificationError(RuntimeError):
    """A member failed verification, so its source object was not deleted."""


@dataclass
class PackPlan:
    """One pack and what is left to delete from it."""

    pack_key: str
    index_key: str
    sidecar_written_at: Optional[datetime] = None
    members: int = 0
    present: int = 0

    @property
    def seq(self) -> str:
        return self.pack_key.rsplit("/", 1)[-1]


@dataclass
class BucketOutcome:
    """Per-bucket counters. Every one of these is reported, including zeros."""

    deleted: int = 0
    bytes_freed: int = 0
    already_gone: int = 0
    verified: int = 0
    refused: int = 0
    skipped_grace: int = 0
    orphan_packs: List[str] = field(default_factory=list)
    failures: List[Dict[str, str]] = field(default_factory=list)
    by_status: Dict[str, int] = field(default_factory=dict)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def bucket_prefix(artifact_type: str, year: int, month: int) -> str:
    """The MinIO prefix holding one monthly capture bucket's source objects."""
    return f"{_HTML_PREFIX}/year={year}/month={month}/artifact_type={artifact_type}/"


def pack_prefix(artifact_type: str, year: int, month: int) -> str:
    """The MinIO prefix holding that bucket's packs and sidecars."""
    return f"{_PACK_PREFIX}/{artifact_type}/{year:04d}/{month:02d}/"


def list_source_objects(
    client: Any, bucket: str, prefix: str, *, progress_every: int = PROGRESS_EVERY
) -> Dict[str, int]:
    """Every surviving ``.html.zst`` object under *prefix*, as key -> size.

    **This listing is the checkpoint**, and it is why there is no state file.
    An object that is gone has already been deleted, so a resumed run skips it
    without a request; there is nothing re-serialised per object, which is the
    O(n^2) shape Plan 129 shipped once and fixed in f98e69b.

    It is also the only source of the byte figures: an object's size is known
    from the listing, so nothing has to be HEADed to report what was freed.
    """
    paginator = client.get_paginator("list_objects_v2")
    out: Dict[str, int] = {}
    started = time.monotonic()
    next_report = progress_every
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for entry in page.get("Contents", []):
            if entry["Key"].endswith(".html.zst"):
                out[entry["Key"]] = entry["Size"]
        if progress_every and len(out) >= next_report:
            elapsed = time.monotonic() - started
            logger.info(
                "delete_packed_source_html: listing %s — %d objects in %.0fs",
                prefix, len(out), elapsed,
            )
            next_report = len(out) + progress_every
    logger.info(
        "delete_packed_source_html: listed %s — %d surviving object(s) in %.0fs",
        prefix, len(out), time.monotonic() - started,
    )
    return out


def list_packs(client: Any, bucket: str, prefix: str) -> Tuple[List[PackPlan], List[str]]:
    """Return (packs with a sidecar, orphan pack keys).

    A pack with no sidecar is an interrupted run. Stage 2 reports and never
    deletes them; nothing here deletes *from* them either — an unverified pack
    is not evidence that anything is safe to remove.
    """
    paginator = client.get_paginator("list_objects_v2")
    packs: Dict[str, None] = {}
    sidecars: Dict[str, datetime] = {}
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for entry in page.get("Contents", []):
            key = entry["Key"]
            if key.endswith(".zpack"):
                packs[key] = None
            elif key.endswith(".idx.parquet"):
                sidecars[key] = entry.get("LastModified")

    plans: List[PackPlan] = []
    orphans: List[str] = []
    for key in sorted(packs):
        sidecar = index_key(key)
        if sidecar not in sidecars:
            orphans.append(key)
            continue
        plans.append(
            PackPlan(pack_key=key, index_key=sidecar, sidecar_written_at=sidecars[sidecar])
        )

    if orphans:
        logger.warning(
            "delete_packed_source_html: %d pack(s) in %s have no sidecar index — an "
            "interrupted run. Nothing will be deleted for them: %s",
            len(orphans), prefix, ", ".join(orphans[:5]),
        )
    return plans, orphans


def _grace_expired(
    plan: PackPlan, grace_days: int, now: Optional[datetime] = None
) -> bool:
    """True when the pack has existed long enough for its sources to go.

    With the default of 0 days this is always true, including for a sidecar
    written seconds ago. A sidecar with no LastModified (an object store that
    does not report one) is treated as eligible: the timestamp is a
    convenience, and per-artifact verification is what actually guards this.
    """
    if grace_days <= 0:
        return True
    if plan.sidecar_written_at is None:
        return True
    written = plan.sidecar_written_at
    if written.tzinfo is None:
        written = written.replace(tzinfo=timezone.utc)
    return (now or _now_utc()) - written >= timedelta(days=grace_days)


# ---------------------------------------------------------------------------
# Processing status — reported, never a veto
# ---------------------------------------------------------------------------

def fetch_terminal_status(
    con: Any, bucket: str, artifact_type: str, year: int, month: int
) -> Dict[int, str]:
    """Latest terminal status per artifact_id, from the events lake.

    The historical record is ``ops_normalized/artifacts_queue_events``, whose
    timestamp column is ``event_at``. It is **not** ``ops.artifacts_queue``,
    which is a hot table pruned by ``cleanup_queue`` — measured at 400 rows,
    all from the preceding 40 seconds, so it cannot say what happened in April.

    Report-only. Nothing here can stop a deletion, so a caller that cannot
    reach DuckDB loses a breakdown, not a safety property.
    """
    window_start = datetime(year, month, 1, tzinfo=timezone.utc) - timedelta(days=2)
    last_day = calendar.monthrange(year, month)[1]
    window_end = datetime(year, month, last_day, tzinfo=timezone.utc) + timedelta(days=3)

    query = f"""
    SELECT artifact_id, status
    FROM (
        SELECT artifact_id, status,
               row_number() OVER (PARTITION BY artifact_id ORDER BY event_at DESC) AS rn
        FROM read_parquet(
            '{_ARTIFACT_EVENTS_PATH.format(bucket=bucket)}',
            hive_partitioning=true, union_by_name=true
        )
        WHERE artifact_type = ?
          AND artifact_id IS NOT NULL
          AND event_at >= ? AND event_at < ?
    )
    WHERE rn = 1
    """
    con.execute(query, [artifact_type, window_start, window_end])
    out: Dict[int, str] = {}
    while True:
        rows = con.fetchmany(10_000)
        if not rows:
            return out
        for artifact_id, status in rows:
            out[int(artifact_id)] = str(status) if status is not None else "unknown"


def status_of(entry: PackIndexEntry, statuses: Dict[int, str]) -> str:
    """The class this member is counted under.

    A sidecar entry with no ``artifact_id`` had no ``artifacts_queue_events``
    row when it was packed — that is exactly how the packer records it — so it
    is the *"packed and verified, no provenance"* class, named rather than
    silently folded into "unknown".
    """
    if entry.artifact_id is None:
        return NO_EVENT_ROW
    return statuses.get(int(entry.artifact_id), NO_EVENT_ROW)


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def _read_source_object(client: Any, bucket: str, key: str) -> Optional[bytes]:
    """Decompressed bytes of the source object, or None if it is already gone.

    Deliberately **not** ``read_html``: with the Stage 3 fallback in place
    ``read_html`` would answer from the pack once the object is missing, and
    this call exists to read the object itself. Comparing the pack against the
    pack would verify nothing.
    """
    from botocore.exceptions import ClientError

    from shared.compression import decompress_frame

    try:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return None
        raise
    return decompress_frame(body)


def verify_member(
    client: Any,
    bucket: str,
    reader: PackReader,
    entry: PackIndexEntry,
    pack_plan: PackPlan,
    *,
    full_read: bool = False,
) -> Tuple[bool, Optional[bytes]]:
    """Prove this member is safe to delete. Returns (source_present, extracted).

    Raises :class:`PruneVerificationError` if anything does not line up. The
    caller counts and continues — one unverifiable member must not stop a run,
    but it must never be deleted either.
    """
    from shared.minio import pack_lookup_prefix, read_packed_html

    key = entry.source_key

    expected_prefix = pack_lookup_prefix(key)
    actual_prefix = pack_plan.pack_key.rsplit("/", 1)[0] + "/"
    if expected_prefix != actual_prefix:
        raise PruneVerificationError(
            f"{key} would resolve to {expected_prefix!r}, but its pack is in "
            f"{actual_prefix!r} — a reader could not find it once the object is gone"
        )

    extracted = reader.read_member(entry)
    digest = hashlib.sha256(extracted).hexdigest()
    if digest != entry.raw_sha256:
        raise PruneVerificationError(
            f"{key}: extracted sha256 {digest[:12]} != sidecar raw_sha256 "
            f"{str(entry.raw_sha256)[:12]}"
        )

    if full_read:
        # The real resolver, end to end: prefix derivation, sidecar listing,
        # index lookup, ranged reads. Sampled per pack rather than per member.
        resolved = read_packed_html(f"s3://{bucket}/{key}")
        if resolved is None:
            raise PruneVerificationError(
                f"{key}: the production read path could not resolve it to any pack"
            )
        if resolved != extracted:
            raise PruneVerificationError(
                f"{key}: the production read path returned different bytes than the "
                f"pack reader did"
            )

    source = _read_source_object(client, bucket, key)
    if source is None:
        return False, extracted
    if source != extracted:
        raise PruneVerificationError(
            f"{key}: the object holds {len(source)} bytes (sha256 "
            f"{hashlib.sha256(source).hexdigest()[:12]}) and the pack holds "
            f"{len(extracted)} (sha256 {digest[:12]}) — refusing to delete"
        )
    return True, extracted


# ---------------------------------------------------------------------------
# Draining one pack
# ---------------------------------------------------------------------------

def _pack_reader(client: Any, bucket: str, pack_key: str) -> PackReader:
    """A reader over the stored pack, using ranged GETs — the read path's shape."""
    size = client.head_object(Bucket=bucket, Key=pack_key)["ContentLength"]

    def fetch(offset: int, length: int) -> bytes:
        end = offset + length - 1
        return client.get_object(
            Bucket=bucket, Key=pack_key, Range=f"bytes={offset}-{end}"
        )["Body"].read()

    return PackReader(fetch, size)


def _sample_positions(count: int, sample: int) -> Set[int]:
    """Evenly spread positions for the full-resolver sample, always incl. the ends.

    Deterministic rather than random: a verification sample that cannot be
    reproduced cannot be argued about afterwards.
    """
    if count <= 0 or sample <= 0:
        return set()
    if sample >= count:
        return set(range(count))
    step = (count - 1) / (sample - 1) if sample > 1 else 1
    return {int(round(i * step)) for i in range(sample)}


def drain_pack(
    client: Any,
    bucket: str,
    plan: PackPlan,
    surviving: Dict[str, int],
    statuses: Dict[int, str],
    outcome: BucketOutcome,
    *,
    apply: bool,
    remaining_budget: int,
    sample_full_reads: int,
) -> int:
    """Verify and delete one pack's sources. Returns objects deleted (or eligible).

    Members are walked in sidecar order, which is frame order, so the reader's
    frame cache turns one frame decompress into one for the whole run of
    members inside it rather than one per member.
    """
    body = client.get_object(Bucket=bucket, Key=plan.index_key)["Body"].read()
    entries = read_index_parquet(body)
    plan.members = len(entries)

    ordered = sorted(entries, key=lambda e: (e.frame_ordinal, e.offset_in_frame))
    pending = [e for e in ordered if e.source_key in surviving]
    plan.present = len(pending)
    if not pending:
        logger.info(
            "delete_packed_source_html: %s — every member's source is already gone",
            plan.seq,
        )
        return 0

    reader = _pack_reader(client, bucket, plan.pack_key)
    if reader.member_count != len(entries):
        # The sidecar and the pack describe different things. Neither is
        # trustworthy as evidence about the other, so nothing here is deleted.
        outcome.failures.append({
            "pack_key": plan.pack_key,
            "error": (
                f"sidecar has {len(entries)} members, pack header says "
                f"{reader.member_count}"
            ),
        })
        outcome.refused += len(pending)
        logger.error(
            "delete_packed_source_html: %s — sidecar/pack disagree (%d vs %d); "
            "refusing to delete any of its %d surviving source(s)",
            plan.seq, len(entries), reader.member_count, len(pending),
        )
        return 0

    sampled = _sample_positions(len(pending), sample_full_reads)
    processed = 0
    for position, entry in enumerate(pending):
        if processed >= remaining_budget:
            break
        try:
            present, _ = verify_member(
                client, bucket, reader, entry, plan,
                full_read=position in sampled,
            )
        except PruneVerificationError as exc:
            outcome.refused += 1
            outcome.failures.append({"source_key": entry.source_key, "error": str(exc)})
            logger.error("delete_packed_source_html: REFUSED %s", exc)
            continue
        except Exception as exc:  # noqa: BLE001 - one bad member must not end the run.
            outcome.refused += 1
            outcome.failures.append({
                "source_key": entry.source_key,
                "error": f"{type(exc).__name__}: {exc}",
            })
            logger.error(
                "delete_packed_source_html: REFUSED %s — %s: %s",
                entry.source_key, type(exc).__name__, exc,
            )
            continue

        outcome.verified += 1
        if not present:
            outcome.already_gone += 1
            continue

        status = status_of(entry, statuses)
        outcome.by_status[status] = outcome.by_status.get(status, 0) + 1
        size = surviving.get(entry.source_key, 0)
        processed += 1

        if not apply:
            continue

        client.delete_object(Bucket=bucket, Key=entry.source_key)
        outcome.deleted += 1
        outcome.bytes_freed += size
        if outcome.deleted % PROGRESS_EVERY == 0:
            logger.info(
                "delete_packed_source_html: %s — %d deleted, %.1f MiB freed",
                plan.seq, outcome.deleted, outcome.bytes_freed / 1024 ** 2,
            )

    return processed


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _free_space(path: str = "/usr/app/logs") -> Dict[str, Any]:
    """Free bytes and inodes. Reported, never a gate.

    The packer refuses to start below a free-space floor because MinIO rejects
    every PutObject below its minimum-free-drive threshold. **Deleting is not
    writing**: a DELETE still succeeds on a full drive, which is precisely what
    makes this job the recovery lever rather than another casualty.
    """
    import shutil

    if not os.path.exists(path):
        path = "/"
    usage = shutil.disk_usage(path)
    reading: Dict[str, Any] = {
        "path": path,
        "free_bytes": usage.free,
        "free_inodes": None,
    }
    statvfs = getattr(os, "statvfs", None)
    if statvfs is not None:
        try:
            reading["free_inodes"] = statvfs(path).f_favail
        except OSError:  # pragma: no cover - platform-dependent
            pass
    return reading


def _finalize(
    result: Dict[str, Any], outcome: BucketOutcome, before: Dict[str, Any]
) -> Dict[str, Any]:
    """Fold the counters into the summary and log it. Every return path uses it.

    A run that aborts part-way, one that finds nothing, and one that completes
    all report the same shape — a summary whose fields depend on which branch
    produced it is a summary an operator has to reverse-engineer.

    Inodes are reported two ways because the plan exists for that number, and
    the two ways answer different questions: the estimate is what *this run*
    freed (deleted objects x ~2.24, measured in Stage 0a), while the measured
    delta is what the filesystem actually shows and therefore moves with
    everything else running on it. A reading, not a proof.
    """
    after = _free_space()
    result["free_space_after"] = after
    result["objects_deleted"] = outcome.deleted
    result["objects_verified"] = outcome.verified
    result["objects_already_gone"] = outcome.already_gone
    result["objects_refused"] = outcome.refused
    result["packs_skipped_grace"] = outcome.skipped_grace
    result["bytes_freed"] = outcome.bytes_freed
    result["inodes_freed_estimated"] = round(outcome.deleted * INODES_PER_OBJECT, 1)
    if before["free_inodes"] is not None and after["free_inodes"] is not None:
        result["inodes_freed_measured"] = after["free_inodes"] - before["free_inodes"]
    result["by_status"] = dict(sorted(outcome.by_status.items()))
    result["orphan_packs"] = outcome.orphan_packs
    result["failures"] = outcome.failures[:50]

    logger.info(
        "delete_packed_source_html: run complete (%s) — deleted=%d verified=%d "
        "refused=%d already_gone=%d bytes=%d inodes~%.0f (measured delta %s) "
        "by_status=%s",
        result["mode"], result["objects_deleted"], result["objects_verified"],
        result["objects_refused"], result["objects_already_gone"],
        result["bytes_freed"], result["inodes_freed_estimated"],
        result["inodes_freed_measured"], result["by_status"],
    )
    return result


def delete_packed_source_html(
    *,
    apply: bool = False,
    artifact_type: str = DEFAULT_ARTIFACT_TYPE,
    year: Optional[int] = None,
    month: Optional[int] = None,
    max_objects: int = MAX_OBJECTS,
    max_packs: int = MAX_PACKS,
    grace_days: int = DELETE_GRACE_DAYS,
    sample_full_reads: int = SAMPLE_FULL_READS,
    status_breakdown: bool = True,
    bucket: str = BUCKET,
) -> Dict[str, Any]:
    """Delete source objects that are provably inside a verified pack.

    **Dry-run unless *apply*.** A dry run performs every verification and every
    read, and deletes nothing — so "would this be safe?" is answerable without
    being irreversible, which is the question worth asking first.

    *year* and *month* are required. There is no discovery mode on purpose:
    this job removes data, and the month it removes data from is an operator's
    decision, not something inferred from what happens to be packed.
    """
    result: Dict[str, Any] = {
        "mode": "apply" if apply else "dry_run",
        "artifact_type": artifact_type,
        "year": year,
        "month": month,
        "grace_days": grace_days,
        "max_objects": max_objects,
        "max_packs": max_packs,
        "packs_considered": 0,
        "packs_drained": 0,
        "objects_surviving_before": 0,
        "objects_deleted": 0,
        "objects_verified": 0,
        "objects_already_gone": 0,
        "objects_refused": 0,
        "packs_skipped_grace": 0,
        "bytes_freed": 0,
        "inodes_freed_estimated": 0.0,
        "inodes_freed_measured": None,
        "by_status": {},
        "orphan_packs": [],
        "free_space_before": None,
        "free_space_after": None,
        "capped": False,
        "failures": [],
        "error": None,
    }

    if year is None or month is None:
        result["error"] = "year and month are required — this job deletes data"
        logger.error("delete_packed_source_html: %s", result["error"])
        return result

    before = _free_space()
    result["free_space_before"] = before
    logger.info(
        "delete_packed_source_html: %s %04d-%02d (%s) — cap %d object(s), %d pack(s), "
        "grace %d day(s); %s free inodes on %s",
        artifact_type, year, month, result["mode"], max_objects, max_packs, grace_days,
        f"{before['free_inodes']:,}" if before["free_inodes"] is not None else "?",
        before["path"],
    )

    client = get_boto3_client()
    outcome = BucketOutcome()

    try:
        plans, orphans = list_packs(client, bucket, pack_prefix(artifact_type, year, month))
        outcome.orphan_packs = orphans
        result["packs_considered"] = len(plans)
        if not plans:
            logger.warning(
                "delete_packed_source_html: no pack with a sidecar under %s — "
                "nothing is deletable here",
                pack_prefix(artifact_type, year, month),
            )
            result["orphan_packs"] = orphans
            return _finalize(result, outcome, before)

        surviving = list_source_objects(
            client, bucket, bucket_prefix(artifact_type, year, month)
        )
        result["objects_surviving_before"] = len(surviving)
        if not surviving:
            # A fully drained month costs one listing and nothing else. Without
            # this, a re-run — or the Stage 5 DAG passing over a month it
            # already finished — would read every sidecar in it to discover
            # there is nothing left to do.
            logger.info(
                "delete_packed_source_html: no surviving source object under %s — "
                "this bucket is fully pruned",
                bucket_prefix(artifact_type, year, month),
            )
            result["orphan_packs"] = orphans
            return _finalize(result, outcome, before)

        statuses: Dict[int, str] = {}
        if status_breakdown:
            statuses = _load_statuses(bucket, artifact_type, year, month)

        budget = max_objects
        for plan in plans:
            if max_packs and result["packs_drained"] >= max_packs:
                result["capped"] = True
                break
            if budget <= 0:
                result["capped"] = True
                break
            if not _grace_expired(plan, grace_days):
                outcome.skipped_grace += 1
                logger.info(
                    "delete_packed_source_html: %s — sidecar written %s, inside the "
                    "%d-day grace period",
                    plan.seq, plan.sidecar_written_at, grace_days,
                )
                continue

            done = drain_pack(
                client, bucket, plan, surviving, statuses, outcome,
                apply=apply,
                remaining_budget=budget,
                sample_full_reads=sample_full_reads,
            )
            budget -= done
            if done:
                result["packs_drained"] += 1
            logger.info(
                "delete_packed_source_html: %s — members=%d surviving=%d handled=%d "
                "deleted_total=%d refused_total=%d",
                plan.seq, plan.members, plan.present, done,
                outcome.deleted, outcome.refused,
            )
        if budget <= 0:
            result["capped"] = True
    except Exception as exc:  # noqa: BLE001 - partial results are still results.
        logger.error("delete_packed_source_html: aborted: %s", exc, exc_info=True)
        result["error"] = str(exc)

    return _finalize(result, outcome, before)


def _load_statuses(
    bucket: str, artifact_type: str, year: int, month: int
) -> Dict[int, str]:
    """Terminal statuses for the breakdown, or an empty map if unavailable.

    Report-only, so a DuckDB failure is logged and swallowed. Letting it end
    the run would make a *reporting* dependency load-bearing on a job whose
    whole design is that status never blocks anything.
    """
    try:
        from shared.duckdb_s3 import get_duckdb_s3_connection

        con = get_duckdb_s3_connection()
        con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
        con.execute(f"SET threads={DUCKDB_THREADS}")
        statuses = fetch_terminal_status(con, bucket, artifact_type, year, month)
        logger.info(
            "delete_packed_source_html: terminal status for %d artifact(s)", len(statuses)
        )
        return statuses
    except Exception as exc:  # noqa: BLE001 - a breakdown is not a safety property.
        logger.warning(
            "delete_packed_source_html: status breakdown unavailable (%s). Deletions "
            "are unaffected — status is reported, never a veto.", exc,
        )
        return {}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Delete bronze HTML objects that are provably inside a verified pack "
            "(Plan 131 Stage 4). Dry-run by default; --apply deletes. The bucket is "
            "un-versioned, so a delete is immediate and there is no undo."
        )
    )
    parser.add_argument("--apply", action="store_true", help="Actually delete objects")
    parser.add_argument("--artifact-type", default=DEFAULT_ARTIFACT_TYPE)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument(
        "--max-objects", type=int, default=MAX_OBJECTS,
        help="Hard per-run cap on objects deleted [default: %(default)s]",
    )
    parser.add_argument(
        "--max-packs", type=int, default=MAX_PACKS,
        help="Packs drained per run; 0 for no cap [default: %(default)s]",
    )
    parser.add_argument(
        "--grace-days", type=int, default=DELETE_GRACE_DAYS,
        help="Days since the sidecar was written [default: %(default)s]",
    )
    parser.add_argument(
        "--sample-full-reads", type=int, default=SAMPLE_FULL_READS,
        help="Members per pack read through the full production resolver",
    )
    parser.add_argument(
        "--no-status-breakdown", action="store_true",
        help="Skip the DuckDB status report (deletions are unaffected either way)",
    )
    parser.add_argument("--json-out", default=None)
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    if not args.apply:
        logger.info(
            "delete_packed_source_html: DRY-RUN — every check runs, nothing is "
            "deleted. Pass --apply to delete."
        )

    result = delete_packed_source_html(
        apply=args.apply,
        artifact_type=args.artifact_type,
        year=args.year,
        month=args.month,
        max_objects=args.max_objects,
        max_packs=args.max_packs,
        grace_days=args.grace_days,
        sample_full_reads=args.sample_full_reads,
        status_breakdown=not args.no_status_breakdown,
    )
    print(json.dumps(result, indent=2, default=str))
    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as handle:
            json.dump(result, handle, indent=2, default=str)
    return 1 if result["error"] or result["objects_refused"] else 0


if __name__ == "__main__":
    sys.exit(main())
