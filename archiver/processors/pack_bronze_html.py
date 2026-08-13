"""Plan 131 Stage 2: pack one cold monthly capture bucket of bronze HTML.

`bronze` is running out of **inodes**, not bytes. Plan 129's trained dictionary
cut bronze bytes ~73% and changed object count by zero; at ~65,500 inodes/day
the ceiling is ~61 days out. This job replaces many small HTML objects with a
few large indexed packs — see ``shared/packfile.py`` for the format.

**Stage 2 deletes nothing.** Packs are written alongside their sources, every
member is verified byte-identical against its own ``raw_sha256`` *after* the
pack is stored, and the source objects are left exactly where they are.
Deleting them is Stage 4 and is the only step that removes data.

Shape follows ``compact_silver``: a pure function returning a summary dict, an
Airflow-triggerable endpoint on the archiver, and a small CLI for manual runs.

Three things this job is careful about, each learned expensively:

* **Free space.** MinIO enforces a minimum-free-drive threshold and refuses
  *every* ``PutObject`` below it — including the small writes that would free
  space (Plan 129 lesson 1). A pack must be written and verified while its
  sources still exist, so it needs its own size free, transiently. The job
  therefore checks the floor before it starts rather than discovering it
  mid-write, and packs default to 64 MB, not GB.
* **The packable universe is MinIO, not silver.** Silver holds ~4.56M detail
  artifacts since April but bronze held ~4.07M objects: some artifacts have no
  surviving HTML (Plan 128's challenge-page eviction is one plausible source).
  Objects are enumerated from the object store; silver only supplies the
  ordering metadata, and an object with no silver row is still packed.
* **The checkpoint is the sidecar index itself.** Resume reads the
  ``.idx.parquet`` files already written for the bucket and subtracts their
  source keys. There is no growing state file re-serialised per object, which
  is the O(n^2) shape Plan 129 shipped once and had to fix (commit f98e69b).
"""
from __future__ import annotations

import argparse
import calendar
import hashlib
import json
import logging
import math
import os
import shutil
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional, Sequence, Set, Tuple

from shared.minio import BUCKET, get_boto3_client
from shared.packfile import (
    DEFAULT_FRAME_TARGET_BYTES,
    PACK_PREFIX,
    PackIndexEntry,
    PackMember,
    PackReader,
    PackWriter,
    index_key,
    pack_key,
    read_index_parquet,
    write_index_parquet,
)

logger = logging.getLogger("archiver")

#: Both are patched to a throwaway namespace by the integration tests, exactly
#: as compact_silver's _MINIO_PREFIX is, so a test run cannot touch (or be
#: displaced by) production objects.
_HTML_PREFIX = "html"
_PACK_PREFIX = PACK_PREFIX
_SILVER_PATH = "s3://{bucket}/silver_normalized/observations/**/*.parquet"
_ARTIFACT_EVENTS_PATH = "s3://{bucket}/ops_normalized/artifacts_queue_events/**/*.parquet"

#: Only artifacts settled for this long are eligible. The scraper write path is
#: unchanged; packing is a background lifecycle job over cold data.
MIN_AGE_DAYS = int(os.environ.get("PACK_MIN_AGE_DAYS", "60"))

#: Monthly capture buckets processed per run (Plan 109's MAX_PARTITIONS analogue).
MAX_BUCKETS = int(os.environ.get("PACK_BRONZE_MAX_BUCKETS", "1"))

#: Packs written per run; 0 means "until the bucket is exhausted". Defaults to
#: one so the first production run is a single, small, reviewable unit of work.
MAX_PACKS = int(os.environ.get("PACK_BRONZE_MAX_PACKS", "1"))

#: Small on purpose: this is the transient free space a pack needs while its
#: sources still exist, and it is the blast radius if a pack is ever lost.
#: Measured in *stored* bytes, and a pack is always at least one frame — a
#: value below one frame's compressed size simply yields one-frame packs.
MAX_PACK_BYTES = int(os.environ.get("PACK_BRONZE_MAX_PACK_BYTES", str(64 * 1024 * 1024)))

#: Uncompressed bytes per frame — the bound on what reading one member costs.
FRAME_TARGET_BYTES = int(
    os.environ.get("PACK_BRONZE_FRAME_BYTES", str(DEFAULT_FRAME_TARGET_BYTES))
)

MIN_FREE_BYTES = int(os.environ.get("PACK_BRONZE_MIN_FREE_BYTES", str(5 * 1024 ** 3)))

#: Filesystem to measure. MinIO's data lives in a docker volume, so the
#: archiver container's own root filesystem sits on the same device.
FREE_SPACE_PATH = os.environ.get("PACK_BRONZE_FREE_SPACE_PATH", "/")

#: DuckDB is bounded here for the same reason Plan 125 bounded it: an
#: unconstrained scan of silver OOMs on the production VM.
DUCKDB_MEMORY_LIMIT = os.environ.get("PACK_BRONZE_DUCKDB_MEMORY", "2GB")
DUCKDB_THREADS = int(os.environ.get("PACK_BRONZE_DUCKDB_THREADS", "2"))

DEFAULT_ARTIFACT_TYPE = "detail_page"

_FETCH_BATCH = 10_000


class DictionaryNotConfiguredError(RuntimeError):
    """Refused to start: no zstd dictionary resolved for pack frames."""


@dataclass
class BucketPlan:
    """One monthly capture bucket and what is left to do in it."""

    artifact_type: str
    year: int
    month: int
    prefix: str
    objects: int = 0
    source_bytes: int = 0
    already_packed: int = 0
    next_seq: int = 0
    orphan_packs: List[str] = field(default_factory=list)

    @property
    def pending(self) -> int:
        return self.objects - self.already_packed


def _today_utc() -> date:
    return datetime.now(timezone.utc).date()


# ---------------------------------------------------------------------------
# Free space
# ---------------------------------------------------------------------------

def free_space(path: str = FREE_SPACE_PATH) -> Dict[str, Any]:
    """Measure free bytes and, where the platform reports them, free inodes.

    Inodes are the constraint this whole plan exists for, so they are reported
    even though the floor is expressed in bytes: a run that frees bytes while
    the inode count keeps climbing is the exact failure Plan 129 recorded.
    """
    usage = shutil.disk_usage(path)
    result: Dict[str, Any] = {
        "path": path,
        "free_bytes": usage.free,
        "total_bytes": usage.total,
        "free_inodes": None,
    }
    statvfs = getattr(os, "statvfs", None)
    if statvfs is not None:
        try:
            stats = statvfs(path)
        except OSError:  # pragma: no cover - platform-dependent
            return result
        result["free_inodes"] = stats.f_favail
    return result


def free_space_status(
    min_free_bytes: int, path: str = FREE_SPACE_PATH
) -> Dict[str, Any]:
    """Measure free space and say whether the job may start.

    ``message`` is populated only when the floor is breached, and says what was
    measured against what — a floor failure that does not name its numbers is
    the kind an operator has to reproduce before they can act on it.
    """
    reading = free_space(path)
    reading["min_free_bytes"] = min_free_bytes
    reading["ok"] = reading["free_bytes"] >= min_free_bytes
    reading["message"] = None
    if not reading["ok"]:
        reading["message"] = (
            f"refusing to start: {reading['free_bytes'] / 1024 ** 3:.2f} GiB free on "
            f"{path}, floor is {min_free_bytes / 1024 ** 3:.2f} GiB. A pack must be "
            f"written and verified while its sources still exist, and MinIO refuses "
            f"every PutObject below its own minimum-free-drive threshold — including "
            f"the small ones that would free space."
        )
    return reading


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def _list_common_prefixes(client: Any, bucket: str, prefix: str) -> List[str]:
    """List immediate child prefixes of *prefix* (one S3 level, not a full walk)."""
    paginator = client.get_paginator("list_objects_v2")
    out: List[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for entry in page.get("CommonPrefixes", []):
            out.append(entry["Prefix"])
    return out


def _parse_hive_int(prefix: str, key: str) -> Optional[int]:
    """Pull ``key=<int>`` out of a hive-style prefix segment."""
    marker = f"{key}="
    if marker not in prefix:
        return None
    try:
        return int(prefix.rstrip("/").rsplit(marker, 1)[1].split("/", 1)[0])
    except ValueError:
        return None


def bucket_prefix(artifact_type: str, year: int, month: int) -> str:
    """The MinIO prefix holding one monthly capture bucket.

    ``make_key`` partitions by the capture's own ``fetched_at``, so the bronze
    hive partition *is* the monthly capture bucket Stage 0d chose. No regrouping
    is needed to pack by month.
    """
    return f"{_HTML_PREFIX}/year={year}/month={month}/artifact_type={artifact_type}/"


def discover_buckets(
    client: Any,
    bucket: str,
    artifact_type: str,
    *,
    min_age_days: int,
    today: Optional[date] = None,
) -> List[Tuple[int, int]]:
    """Return (year, month) buckets whose every artifact is older than the floor.

    A bucket qualifies only when its month-end is past the age floor, so a
    bucket is never packed while it can still receive writes.
    """
    today = today or _today_utc()
    cutoff = today - timedelta(days=min_age_days)

    eligible: List[Tuple[int, int]] = []
    for year_prefix in _list_common_prefixes(client, bucket, f"{_HTML_PREFIX}/"):
        year = _parse_hive_int(year_prefix, "year")
        if year is None:
            continue
        for month_prefix in _list_common_prefixes(client, bucket, year_prefix):
            month = _parse_hive_int(month_prefix, "month")
            if month is None or not 1 <= month <= 12:
                continue
            month_end = date(year, month, calendar.monthrange(year, month)[1])
            if month_end > cutoff:
                continue
            type_prefix = bucket_prefix(artifact_type, year, month)
            if _list_objects(client, bucket, type_prefix, limit=1):
                eligible.append((year, month))
    return sorted(eligible)


def _list_objects(
    client: Any, bucket: str, prefix: str, *, limit: int = 0
) -> List[Tuple[str, int]]:
    """List ``.html.zst`` objects under *prefix* as (key, size)."""
    paginator = client.get_paginator("list_objects_v2")
    out: List[Tuple[str, int]] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for entry in page.get("Contents", []):
            if entry["Key"].endswith(".html.zst"):
                out.append((entry["Key"], entry["Size"]))
                if limit and len(out) >= limit:
                    return out
    return out


def _pack_state(
    client: Any, bucket: str, artifact_type: str, year: int, month: int
) -> Tuple[Set[str], int, List[str]]:
    """Read the bucket's existing sidecars: (packed source keys, next seq, orphans).

    The sidecars *are* the checkpoint. A pack is only ever written after every
    one of its members verified, and its index is written after the pack, so a
    key present in a sidecar is a key that is provably already packed. Cost is
    one read per existing pack — O(members), never re-serialised per object.

    A pack object with no sidecar is an interrupted run. It is reported and its
    sequence number is respected so nothing overwrites it, but it is otherwise
    ignored: nothing references it, and Stage 2 deletes nothing.
    """
    prefix = f"{_PACK_PREFIX}/{artifact_type}/{year:04d}/{month:02d}/"
    paginator = client.get_paginator("list_objects_v2")
    packs: Set[str] = set()
    indexes: Set[str] = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for entry in page.get("Contents", []):
            key = entry["Key"]
            if key.endswith(".zpack"):
                packs.add(key)
            elif key.endswith(".idx.parquet"):
                indexes.add(key)

    packed_keys: Set[str] = set()
    for key in sorted(indexes):
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        packed_keys.update(entry.source_key for entry in read_index_parquet(body))

    orphans = sorted(p for p in packs if index_key(p) not in indexes)
    if orphans:
        logger.warning(
            "pack_bronze_html: %d pack(s) in %s have no sidecar index — an "
            "interrupted run. They are unreferenced and are being ignored, not "
            "deleted: %s",
            len(orphans), prefix, ", ".join(orphans[:5]),
        )

    seqs = [_parse_seq(k) for k in packs]
    next_seq = max([s for s in seqs if s is not None], default=-1) + 1
    return packed_keys, next_seq, orphans


def _parse_seq(key: str) -> Optional[int]:
    stem = key.rsplit("/", 1)[-1]
    if not stem.startswith("pack-") or not stem.endswith(".zpack"):
        return None
    try:
        return int(stem[len("pack-"): -len(".zpack")])
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Member ordering
# ---------------------------------------------------------------------------

def fetch_member_metadata(
    con: Any, bucket: str, artifact_type: str, year: int, month: int
) -> Iterator[Tuple[str, Optional[int], Optional[str], Optional[datetime]]]:
    """Stream (source_key, artifact_id, listing_id, fetched_at) for one bucket.

    Ordered ``listing_id, fetched_at``: Stage 0d measured that a vehicle's
    repeat captures must be adjacent inside one frame, because that is where
    the redundancy is (30.4% within a listing against 0.65% across listings).

    Results are streamed rather than materialised — a busy month holds several
    hundred thousand artifacts, and DuckDB spills its sort while Python would
    not.
    """
    key_prefix = f"s3://{bucket}/{bucket_prefix(artifact_type, year, month)}"
    window_start = datetime(year, month, 1, tzinfo=timezone.utc) - timedelta(days=2)
    last_day = calendar.monthrange(year, month)[1]
    window_end = datetime(year, month, last_day, tzinfo=timezone.utc) + timedelta(days=3)

    query = f"""
    WITH paths AS (
        SELECT artifact_id, any_value(minio_path) AS minio_path
        FROM read_parquet(
            '{_ARTIFACT_EVENTS_PATH.format(bucket=bucket)}',
            hive_partitioning=true, union_by_name=true
        )
        WHERE artifact_type = ?
          AND minio_path IS NOT NULL
          AND starts_with(minio_path, ?)
        GROUP BY artifact_id
    ),
    obs AS (
        SELECT artifact_id,
               any_value(listing_id) AS listing_id,
               min(fetched_at)       AS fetched_at
        FROM read_parquet(
            '{_SILVER_PATH.format(bucket=bucket)}',
            hive_partitioning=true, union_by_name=true
        )
        WHERE artifact_id IS NOT NULL
          AND fetched_at >= ? AND fetched_at < ?
        GROUP BY artifact_id
    )
    SELECT p.minio_path, p.artifact_id, o.listing_id, o.fetched_at
    FROM paths p
    LEFT JOIN obs o USING (artifact_id)
    ORDER BY o.listing_id, o.fetched_at, p.artifact_id
    """
    con.execute(query, [artifact_type, key_prefix, window_start, window_end])
    while True:
        rows = con.fetchmany(_FETCH_BATCH)
        if not rows:
            return
        for minio_path, artifact_id, listing_id, fetched_at in rows:
            yield (
                str(minio_path).split(f"s3://{bucket}/", 1)[-1],
                int(artifact_id) if artifact_id is not None else None,
                str(listing_id) if listing_id is not None else None,
                fetched_at,
            )


def iter_ordered_keys(
    metadata: Iterator[Tuple[str, Optional[int], Optional[str], Optional[datetime]]],
    remaining: Set[str],
) -> Iterator[Tuple[str, Optional[int], Optional[str], Optional[datetime]]]:
    """Yield packable objects in listing order, then whatever silver never saw.

    *remaining* is consumed as it goes, so a duplicate metadata row cannot pack
    the same object twice and the leftovers at the end are exactly the objects
    with no silver row. Those are still packed — an object nobody can describe
    is still an inode, and leaving it out would leave it unpackable forever.
    """
    for source_key, artifact_id, listing_id, fetched_at in metadata:
        if source_key in remaining:
            remaining.discard(source_key)
            yield source_key, artifact_id, listing_id, fetched_at

    for source_key in sorted(remaining):
        yield source_key, None, None, None
    remaining.clear()


# ---------------------------------------------------------------------------
# Packing
# ---------------------------------------------------------------------------

def _verify_stored_pack(
    client: Any, bucket: str, key: str, entries: Sequence[PackIndexEntry], size: int
) -> int:
    """Re-extract every member from the *stored* object and check its sha256.

    ``PackWriter.finish`` already verified the bytes in memory. This verifies
    what MinIO actually holds, which is the claim Stage 4 will one day delete
    source objects on the strength of. Reads are ranged, one frame at a time.
    """
    def fetch(offset: int, length: int) -> bytes:
        end = offset + length - 1
        response = client.get_object(Bucket=bucket, Key=key, Range=f"bytes={offset}-{end}")
        return response["Body"].read()

    reader = PackReader(fetch, size, max_cached_frames=1)
    reader.check_index(entries)

    verified = 0
    for entry in sorted(entries, key=lambda e: (e.frame_ordinal, e.offset_in_frame)):
        extracted = reader.read_member(entry)
        if hashlib.sha256(extracted).hexdigest() != entry.raw_sha256:
            raise RuntimeError(
                f"stored pack {key}: member {entry.source_key} did not extract "
                f"byte-identically"
            )
        verified += 1
    return verified


def _write_one_pack(
    client: Any,
    bucket: str,
    plan: BucketPlan,
    writer: PackWriter,
    seq: int,
    raw_bytes: int,
) -> Dict[str, Any]:
    """Finalize, store, and re-verify one pack. Deletes nothing."""
    pack = writer.finish()

    key = pack_key(plan.artifact_type, plan.year, plan.month, seq, prefix=_PACK_PREFIX)
    client.put_object(
        Bucket=bucket, Key=key, Body=pack.data, ContentType="application/zstd"
    )

    verified = _verify_stored_pack(client, bucket, key, pack.entries, pack.size)

    sidecar = index_key(key)
    client.put_object(
        Bucket=bucket,
        Key=sidecar,
        Body=write_index_parquet(pack.entries),
        ContentType="application/vnd.apache.parquet",
    )

    logger.info(
        "pack_bronze_html: wrote %s — members=%d frames=%d pack_bytes=%d raw_bytes=%d "
        "verified=%d dictionary_id=%s",
        key, pack.member_count, pack.frame_count, pack.size, raw_bytes,
        verified, pack.dict_id,
    )
    return {
        "pack_key": key,
        "index_key": sidecar,
        "members": pack.member_count,
        "frames": pack.frame_count,
        "pack_bytes": pack.size,
        "raw_bytes": raw_bytes,
        "verified_members": verified,
    }


def _pack_bucket(
    client: Any,
    bucket: str,
    plan: BucketPlan,
    ordered: Iterator[Tuple[str, Optional[int], Optional[str], Optional[datetime]]],
    *,
    dict_id: Optional[int],
    max_packs: int,
    max_pack_bytes: int,
    frame_target_bytes: int,
) -> Dict[str, Any]:
    """Pack one bucket, one pack at a time: build → store → verify → next."""
    from shared.minio import read_html

    packs: List[Dict[str, Any]] = []
    read_failures: List[Dict[str, str]] = []
    seq = plan.next_seq
    stopped_early = False

    # Members go straight into the writer rather than into a list first, so the
    # roll decision can be made on *compressed* bytes. Cutting on raw bytes
    # would be ~20x conservative — detail pages are ~158 KB raw against ~7.3 KB
    # stored — and max_pack_bytes exists to bound the transient free space a
    # pack needs, which is a compressed-bytes quantity. It also keeps memory
    # bounded: sealing a frame releases the raw content behind it.
    writer: Optional[PackWriter] = None
    raw_bytes = 0

    def flush() -> bool:
        """Store the open pack, if any. Returns False when the pack cap is hit."""
        nonlocal writer, raw_bytes, seq
        if writer is None or writer.member_count == 0:
            return True
        packs.append(_write_one_pack(client, bucket, plan, writer, seq, raw_bytes))
        seq += 1
        writer = None
        raw_bytes = 0
        return not (max_packs and len(packs) >= max_packs)

    # A failure part-way through must still report the packs that were written
    # and verified before it: they are finalized, indexed, and will be skipped
    # on resume, so a report that omits them describes a state that never
    # existed.
    error: Optional[str] = None
    try:
        for source_key, artifact_id, listing_id, fetched_at in ordered:
            try:
                content = read_html(f"s3://{bucket}/{source_key}")
            except Exception as exc:  # noqa: BLE001 - one bad object must not end the run.
                logger.warning("pack_bronze_html: read failed key=%s: %s", source_key, exc)
                read_failures.append({"source_key": source_key, "error": str(exc)})
                continue

            if writer is None:
                writer = PackWriter(
                    dict_id=dict_id, frame_target_bytes=frame_target_bytes
                )
            writer.add(
                PackMember(
                    source_key=source_key,
                    content=content,
                    artifact_id=artifact_id,
                    listing_id=listing_id,
                    fetched_at=fetched_at,
                )
            )
            raw_bytes += len(content)
            # compressed_bytes counts sealed frames only, so a pack overshoots
            # its target by at most one frame.
            if writer.compressed_bytes >= max_pack_bytes:
                if not flush():
                    stopped_early = True
                    break

        # flush() returns False only when the cap has just been reached, and
        # that path breaks out above — so reaching here means the cap is still
        # short by at least one pack and this tail write cannot exceed it.
        if not stopped_early:
            flush()
    except Exception as exc:  # noqa: BLE001 - reported with the partial results.
        logger.error(
            "pack_bronze_html: bucket %s aborted after %d pack(s): %s",
            plan.prefix, len(packs), exc, exc_info=True,
        )
        error = str(exc)

    return {
        "packs": packs,
        "read_failures": read_failures,
        "stopped_at_max_packs": stopped_early,
        "error": error,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _resolve_dictionary(dict_id: Optional[int], allow_no_dictionary: bool) -> Optional[int]:
    """Resolve the dictionary for pack frames, before a single object is read.

    ``HTML_COMPRESSION_DICT_ID`` is set on the *writer* (the scraper), not on
    readers, so ``configured_dictionary_id()`` returns nothing in the archiver.
    ``PACK_BRONZE_DICT_ID`` is how the packer is told, and an unset one is
    refused rather than silently writing undictionaried packs: Stage 0d
    measured the first member of each frame growing from 8,578 to 32,952 bytes
    without it, which is ~10 GiB across 441,448 groups.
    """
    from shared.compression import configured_dictionary_id, get_dictionary

    if dict_id is None:
        env = os.environ.get("PACK_BRONZE_DICT_ID", "").strip()
        dict_id = int(env) if env else configured_dictionary_id()

    if not dict_id:
        if not allow_no_dictionary:
            raise DictionaryNotConfiguredError(
                "no zstd dictionary resolved for pack frames. Set PACK_BRONZE_DICT_ID "
                "(HTML_COMPRESSION_DICT_ID is set on the scraper, not on readers), pass "
                "dict_id explicitly, or pass allow_no_dictionary=True if an "
                "undictionaried pack is genuinely intended."
            )
        logger.warning(
            "pack_bronze_html: writing pack frames with NO dictionary — the first "
            "member of every frame will be ~4x larger than it needs to be."
        )
        return None

    registered = get_dictionary(dict_id)
    logger.info(
        "pack_bronze_html: dictionary %d resolved from %s (%d bytes)",
        registered.dict_id, registered.source, len(registered.raw),
    )
    return dict_id


def pack_bronze_html(
    *,
    apply: bool = False,
    artifact_type: str = DEFAULT_ARTIFACT_TYPE,
    year: Optional[int] = None,
    month: Optional[int] = None,
    max_buckets: int = MAX_BUCKETS,
    max_packs: int = MAX_PACKS,
    max_pack_bytes: int = MAX_PACK_BYTES,
    frame_target_bytes: int = FRAME_TARGET_BYTES,
    min_age_days: int = MIN_AGE_DAYS,
    min_free_bytes: int = MIN_FREE_BYTES,
    dict_id: Optional[int] = None,
    allow_no_dictionary: bool = False,
    bucket: str = BUCKET,
) -> Dict[str, Any]:
    """Pack eligible cold monthly capture buckets. Dry-run unless *apply*.

    Returns a summary dict plus a per-bucket breakdown. **No source object is
    ever deleted by this function**, in either mode.
    """
    result: Dict[str, Any] = {
        "mode": "apply" if apply else "dry_run",
        "artifact_type": artifact_type,
        "buckets_eligible": 0,
        "buckets_processed": 0,
        "objects_pending": 0,
        "packs_written": 0,
        "members_packed": 0,
        "members_verified": 0,
        "read_failures": 0,
        "pack_bytes": 0,
        "source_bytes": 0,
        "free_space": None,
        "error": None,
        "buckets": [],
    }

    reading = free_space_status(min_free_bytes, FREE_SPACE_PATH)
    result["free_space"] = reading
    if not reading["ok"]:
        if apply:
            logger.error("pack_bronze_html: %s", reading["message"])
            result["error"] = reading["message"]
            return result
        # A dry-run writes nothing, and "how much would packing this month
        # free?" is exactly the question worth asking when the disk is full.
        logger.warning("pack_bronze_html: an apply run would refuse — %s", reading["message"])

    try:
        resolved_dict_id = _resolve_dictionary(dict_id, allow_no_dictionary)
    except Exception as exc:  # noqa: BLE001 - reported like any other run failure.
        logger.error("pack_bronze_html: %s", exc)
        result["error"] = str(exc)
        return result

    client = get_boto3_client()

    if year is not None and month is not None:
        candidates = [(year, month)]
        month_end = date(year, month, calendar.monthrange(year, month)[1])
        if month_end > _today_utc() - timedelta(days=min_age_days):
            # Not refused: naming a bucket explicitly is a deliberate act, and
            # Stage 2 deletes nothing, so the worst case is that later captures
            # in the month are packed by a later run. It is still worth saying.
            logger.warning(
                "pack_bronze_html: %04d-%02d is inside the %d-day age floor and may "
                "still be receiving writes",
                year, month, min_age_days,
            )
    else:
        candidates = discover_buckets(
            client, bucket, artifact_type, min_age_days=min_age_days
        )
    result["buckets_eligible"] = len(candidates)
    if not candidates:
        logger.info(
            "pack_bronze_html: no bucket older than %d days under %s/",
            min_age_days, _HTML_PREFIX,
        )
        return result

    for target_year, target_month in candidates[:max_buckets]:
        summary = _process_bucket(
            client, bucket, artifact_type, target_year, target_month,
            apply=apply,
            dict_id=resolved_dict_id,
            max_packs=max_packs,
            max_pack_bytes=max_pack_bytes,
            frame_target_bytes=frame_target_bytes,
        )
        result["buckets"].append(summary)
        result["buckets_processed"] += 1
        result["objects_pending"] += summary["pending"]
        result["source_bytes"] += summary["source_bytes"]
        result["packs_written"] += len(summary["packs"])
        result["members_packed"] += sum(p["members"] for p in summary["packs"])
        result["members_verified"] += sum(p["verified_members"] for p in summary["packs"])
        result["pack_bytes"] += sum(p["pack_bytes"] for p in summary["packs"])
        result["read_failures"] += len(summary["read_failures"])
        if summary["error"]:
            result["error"] = summary["error"]
            break

    logger.info(
        "pack_bronze_html: run complete (%s) — buckets=%d pending=%d packs=%d "
        "members=%d verified=%d failures=%d",
        result["mode"], result["buckets_processed"], result["objects_pending"],
        result["packs_written"], result["members_packed"],
        result["members_verified"], result["read_failures"],
    )
    return result


def _process_bucket(
    client: Any,
    bucket: str,
    artifact_type: str,
    year: int,
    month: int,
    *,
    apply: bool,
    dict_id: Optional[int],
    max_packs: int,
    max_pack_bytes: int,
    frame_target_bytes: int,
) -> Dict[str, Any]:
    prefix = bucket_prefix(artifact_type, year, month)
    plan = BucketPlan(artifact_type=artifact_type, year=year, month=month, prefix=prefix)

    objects = _list_objects(client, bucket, prefix)
    plan.objects = len(objects)
    plan.source_bytes = sum(size for _, size in objects)
    remaining = {key for key, _ in objects}

    packed_keys, next_seq, orphans = _pack_state(client, bucket, artifact_type, year, month)
    plan.next_seq = next_seq
    plan.orphan_packs = orphans
    before = len(remaining)
    remaining -= packed_keys
    plan.already_packed = before - len(remaining)

    summary: Dict[str, Any] = {
        "artifact_type": artifact_type,
        "year": year,
        "month": month,
        "prefix": prefix,
        "objects": plan.objects,
        "source_bytes": plan.source_bytes,
        "already_packed": plan.already_packed,
        "pending": len(remaining),
        "next_seq": next_seq,
        "orphan_packs": orphans,
        "packs": [],
        "read_failures": [],
        "stopped_at_max_packs": False,
        "error": None,
    }

    logger.info(
        "pack_bronze_html: bucket %s — objects=%d already_packed=%d pending=%d "
        "source_bytes=%d next_seq=%d",
        prefix, plan.objects, plan.already_packed, len(remaining),
        plan.source_bytes, next_seq,
    )

    if not apply:
        # Upper bound: source objects are already individually compressed, so a
        # pack of them is smaller than their sum, never larger.
        summary["estimated_packs_upper_bound"] = (
            math.ceil(
                sum(size for key, size in objects if key in remaining) / max_pack_bytes
            )
            if remaining else 0
        )
        return summary

    if not remaining:
        return summary

    try:
        from shared.duckdb_s3 import get_duckdb_s3_connection

        con = get_duckdb_s3_connection()
        con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
        con.execute(f"SET threads={DUCKDB_THREADS}")
        ordered = iter_ordered_keys(
            fetch_member_metadata(con, bucket, artifact_type, year, month), remaining
        )
        outcome = _pack_bucket(
            client, bucket, plan, ordered,
            dict_id=dict_id,
            max_packs=max_packs,
            max_pack_bytes=max_pack_bytes,
            frame_target_bytes=frame_target_bytes,
        )
    except Exception as exc:  # noqa: BLE001 - one bucket's failure is reported, not raised.
        logger.error(
            "pack_bronze_html: bucket %s failed: %s", prefix, exc, exc_info=True
        )
        summary["error"] = str(exc)
        return summary

    summary["packs"] = outcome["packs"]
    summary["read_failures"] = outcome["read_failures"]
    summary["stopped_at_max_packs"] = outcome["stopped_at_max_packs"]
    summary["error"] = outcome["error"]
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Pack cold bronze HTML into indexed pack objects (Plan 131 Stage 2). "
            "Dry-run by default; --apply writes packs. Never deletes anything."
        )
    )
    parser.add_argument("--apply", action="store_true", help="Write packs to MinIO")
    parser.add_argument("--artifact-type", default=DEFAULT_ARTIFACT_TYPE)
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--month", type=int, default=None)
    parser.add_argument("--max-buckets", type=int, default=MAX_BUCKETS)
    parser.add_argument(
        "--max-packs", type=int, default=MAX_PACKS,
        help="Packs written per run; 0 for no cap [default: %(default)s]",
    )
    parser.add_argument("--max-pack-bytes", type=int, default=MAX_PACK_BYTES)
    parser.add_argument("--frame-bytes", type=int, default=FRAME_TARGET_BYTES)
    parser.add_argument("--min-age-days", type=int, default=MIN_AGE_DAYS)
    parser.add_argument("--min-free-bytes", type=int, default=MIN_FREE_BYTES)
    parser.add_argument(
        "--dict-id", type=int, default=None,
        help="zstd dictionary for pack frames [default: $PACK_BRONZE_DICT_ID]",
    )
    parser.add_argument(
        "--allow-no-dictionary", action="store_true",
        help="Permit undictionaried packs. Refused by default.",
    )
    parser.add_argument("--json-out", default=None)
    args = parser.parse_args(argv)
    if (args.year is None) != (args.month is None):
        parser.error("--year and --month must be given together")
    return args


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    if not args.apply:
        logger.info("pack_bronze_html: DRY-RUN — no writes. Pass --apply to write.")

    result = pack_bronze_html(
        apply=args.apply,
        artifact_type=args.artifact_type,
        year=args.year,
        month=args.month,
        max_buckets=args.max_buckets,
        max_packs=args.max_packs,
        max_pack_bytes=args.max_pack_bytes,
        frame_target_bytes=args.frame_bytes,
        min_age_days=args.min_age_days,
        min_free_bytes=args.min_free_bytes,
        dict_id=args.dict_id,
        allow_no_dictionary=args.allow_no_dictionary,
    )
    print(json.dumps(result, indent=2, default=str))
    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as handle:
            json.dump(result, handle, indent=2, default=str)
    return 1 if result["error"] or result["read_failures"] else 0


if __name__ == "__main__":
    sys.exit(main())
