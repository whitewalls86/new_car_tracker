"""What ``archiver``'s routes return.

Plan 162 Stage AA, gap G31. See ``shared/api_models.py`` for the filtering
hazard these are written against.

**The three pack jobs were the ones expected to resist a model, and they did
not.** ``pack_bronze_html``, ``delete_packed_source_html`` and
``verify_pack_read_path`` build their summary incrementally across hundreds of
lines, which reads like a shape that varies by branch. It does not: each opens
with one dict literal holding every key it will ever answer -- 16, 26 and 9 --
and no return path adds another. ``_finalize``'s own docstring says why, and it
was written long before this stage: *"A run that aborts part-way, one that
finds nothing, and one that completes all report the same shape -- a summary
whose fields depend on which branch produced it is a summary an operator has to
reverse-engineer."* These models are that decision written where a caller can
see it.

**Nested collections are typed where their element shape is a fact of this
package** -- a compacted partition, a written pack, a flushed table -- and left
as free-form objects where the element is another module's business. A model
that guessed at the second kind would filter keys out of it.
"""
from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Shared sub-objects
# ---------------------------------------------------------------------------

class FreeSpace(BaseModel):
    """``free_space_status()`` -- the floor check every long job runs first.

    ``free_inodes`` is ``None`` on platforms without ``statvfs``, which is not
    hypothetical: this repository is developed on Windows and runs on Linux.
    ``message`` is set only when the floor is breached.
    """

    path: str
    free_bytes: int
    total_bytes: int
    free_inodes: int | None = None
    min_free_bytes: int | None = None
    ok: bool | None = None
    message: str | None = None


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

class CleanupOutcome(BaseModel):
    """One artifacts_queue row's fate. ``reason`` is set only on a refusal."""

    artifact_id: int
    deleted: bool
    reason: str | None = None


class CleanupResponse(BaseModel):
    """``POST /cleanup/queue`` and ``POST /cleanup/queue/run``.

    The batch endpoint takes a caller-supplied id list and the run endpoint
    sweeps every eligible row; both answer the same four counters, and only the
    sweep can carry an ``error``.
    """

    total: int
    deleted: int
    failed: int
    results: List[CleanupOutcome]
    error: str | None = None


# ---------------------------------------------------------------------------
# Flush
# ---------------------------------------------------------------------------

class FlushSilverResponse(BaseModel):
    """``POST /flush/silver/run`` -- still warning-only, so a bad run is a 200.

    Plan 134 Stage C deploy 3 of 3 turns that warning into a 500 carrying a
    ``failure_reason``. When that lands this model gains the field; declaring
    it today would be declaring a key nothing emits.
    """

    flushed: int
    error: str | None = None


class FlushedTable(BaseModel):
    """One staging table's flush outcome."""

    table: str
    flushed: int
    error: str | None = None


class FlushStagingResponse(BaseModel):
    """``POST /flush/staging/run``."""

    tables: List[FlushedTable]
    total_flushed: int
    error: str | None = None


# ---------------------------------------------------------------------------
# Compaction
# ---------------------------------------------------------------------------

class CompactedPartition(BaseModel):
    """One partition's compaction, as ``_compact_one`` reports it."""

    source: str
    date: str
    state: str
    ok: bool
    rows: int | None = None
    files_merged: int | None = None
    size_before_bytes: int | None = None
    size_after_bytes: int | None = None
    error: str | None = None


class CompactSilverResponse(BaseModel):
    """``POST /compact/silver/run`` -- enforced, so a failing run is a 500."""

    scanned: int
    compacted: int
    skipped: int
    failed: int
    incremental: int
    partitions: List[CompactedPartition]
    size_before_mb: float | None = None
    size_after_mb: float | None = None
    error: str | None = None


# ---------------------------------------------------------------------------
# Packing
# ---------------------------------------------------------------------------

class WrittenPack(BaseModel):
    """One pack file, as ``_write_one_pack`` reports it."""

    pack_key: str
    index_key: str
    members: int
    verified_members: int
    frames: int
    pack_bytes: int
    raw_bytes: int


class PackedBucket(BaseModel):
    """One month bucket's packing run."""

    packs: List[WrittenPack]
    read_failures: int
    stopped_at_max_packs: bool
    stopped_for_deploy: bool
    error: str | None = None


class PackBronzeResponse(BaseModel):
    """``POST /pack/bronze/run`` -- the 16 keys its opening literal declares.

    ``mode`` is ``"apply"`` or ``"dry_run"``; this endpoint never deletes a
    source object in either.
    """

    mode: str
    artifact_type: str | None = None
    repacking: bool
    buckets_eligible: int
    buckets_processed: int
    objects_pending: int
    packs_written: int
    members_packed: int
    members_verified: int
    read_failures: int
    pack_bytes: int
    source_bytes: int
    free_space: FreeSpace | None = None
    stopped_for_deploy: bool
    buckets: List[PackedBucket]
    error: str | None = None


class PrunePackedResponse(BaseModel):
    """``POST /pack/bronze/prune`` -- the 26 keys ``_finalize`` guarantees.

    The only endpoint on this service that removes bronze data. The bucket is
    un-versioned, so a delete is immediate and there is no undo, which is why
    the summary reports what survived as well as what went.

    Inodes are reported twice on purpose: ``inodes_freed_estimated`` is what
    *this run* freed (deleted objects x ~2.24, measured in Plan 131 Stage 0a)
    and ``inodes_freed_measured`` is the filesystem delta, which moves with
    everything else on the host. A reading, not a proof.
    """

    mode: str
    year: int
    month: int
    artifact_type: str | None = None
    grace_days: int
    max_objects: int | None = None
    max_packs: int | None = None
    packs_considered: int
    packs_drained: int
    packs_skipped_grace: int
    orphan_packs: List[str]
    objects_surviving_before: int | None = None
    objects_deleted: int
    objects_verified: int
    objects_already_gone: int
    objects_refused: int
    bytes_freed: int
    inodes_freed_estimated: float | None = None
    inodes_freed_measured: int | None = None
    free_space_before: FreeSpace | None = None
    free_space_after: FreeSpace | None = None
    by_status: Dict[str, int]
    failures: List[Dict[str, Any]]
    capped: bool
    stopped_for_deploy: bool
    error: str | None = None


class Percentiles(BaseModel):
    """A latency distribution: ``n`` samples, p50/p95/max in milliseconds."""

    n: int
    p50: float | None = None
    p95: float | None = None
    max: float | None = None


class VerifyPackResponse(BaseModel):
    """``POST /pack/bronze/verify`` -- a read-only canary over a packed month.

    Deliberately outside the pack and prune single-flight slots: a canary must
    stay available while either mutation is running, which is when it is most
    useful.
    """

    bucket: str
    prefix: str
    sampled: int
    verified: int
    failed: int
    sidecars: int
    sources_already_deleted: int
    latency_ms: Dict[str, Percentiles]
    failures: List[Dict[str, Any]]


# ---------------------------------------------------------------------------
# Disk usage
# ---------------------------------------------------------------------------

class DiskReading(BaseModel):
    """One watchlist target's measurement.

    ``carried_forward`` marks a reading reused from the previous run because
    this one did not schedule it -- which is why ``error`` can be the literal
    ``"not scheduled this run"`` and mean nothing is wrong.
    """

    metric: str
    target: str
    bytes: int | None = None
    measured_at: float | None = None
    carried_forward: bool
    error: str | None = None


class DiskUsageResponse(BaseModel):
    """``POST /disk-usage/run`` -- measures the watchlist, publishes via node-exporter.

    ``unpublished`` is the *names* of the targets whose measurement was lost,
    not a count of them. Worth stating: every other field here is a counter,
    and this stage typed it as one on the first pass until the suite said
    otherwise.
    """

    measured: int
    failed: int
    unpublished: List[str]
    carried_forward: int
    include_slow: bool
    textfile: str | None = None
    results: List[DiskReading]


# ---------------------------------------------------------------------------
# Snapshot export
# ---------------------------------------------------------------------------

class SnapshotExportResponse(BaseModel):
    """``POST /snapshots/adaptive-refresh/run`` -- ``SnapshotResult.to_dict()``.

    **Transcribed from the dataclass, after transcribing it by eye got it
    wrong.** The first version of this model listed 19 fields, read off a
    truncated grep of ``to_dict``. The dataclass declares 28. The nine missing
    ones were the whole caching layer -- ``export_cache_hit``,
    ``archive_sha256``, ``materialized_snapshot_path`` and the rest -- and
    FastAPI would have deleted every one of them from this endpoint's response
    in production, silently, with the generated contract agreeing because the
    contract is generated from this file.

    ``tests/plugins/response_model_fidelity.py`` caught it on its first run.
    That is the argument for the plugin, written where the next person
    tempted to hand-maintain one of these will read it.

    The diagnostics and audit fields stay free-form objects on purpose: their
    shape belongs to the selector and cohort modules, and a model that guessed
    at it here would filter keys out of somebody else's report.
    """

    snapshot_id: str
    tier: str
    status: str
    source_window_start: str | None = None
    source_window_end: str | None = None
    seed_vin_count: int | None = None
    closed_vin_count: int | None = None
    listing_count: int | None = None
    artifact_count: int | None = None
    archive_bytes: int | None = None
    manifest_key: str | None = None
    archive_key: str | None = None
    coverage_failures: List[str] = []
    source_audit: Dict[str, Any] | None = None
    selector_diagnostics: Dict[str, Any] | None = None
    cohort_diagnostics: Dict[str, Any] | None = None
    planning_cache_key: str | None = None
    planning_cache_path: str | None = None
    planning_cache_hit: bool = False
    planning_cache_action: str | None = None
    export_fingerprint: str | None = None
    export_cache_hit: bool = False
    export_cache_action: str | None = None
    materialized_snapshot_path: str | None = None
    archive_manifest_key: str | None = None
    archive_sha256: str | None = None
    archive_cache_hit: bool = False
    archive_cache_action: str | None = None
