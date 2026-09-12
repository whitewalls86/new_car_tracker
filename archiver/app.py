import logging
import os
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import Body, FastAPI, HTTPException

from archiver.api_models import (
    CleanupResponse,
    CompactSilverResponse,
    DiskUsageResponse,
    FlushSilverResponse,
    FlushStagingResponse,
    PackBronzeResponse,
    PrunePackedResponse,
    SnapshotExportResponse,
    VerifyPackResponse,
)
from archiver.processors.cleanup_queue import cleanup_queue as _cleanup_queue
from archiver.processors.cleanup_queue import run_cleanup_queue as _run_cleanup_queue
from archiver.processors.compact_silver import compact_silver as _compact_silver
from archiver.processors.delete_packed_source_html import (
    delete_packed_source_html as _delete_packed_source_html,
)
from archiver.processors.disk_usage import run_disk_usage as _run_disk_usage
from archiver.processors.disk_usage import textfile_dir as _disk_usage_textfile_dir
from archiver.processors.export_ci_lake_snapshot import (
    SnapshotRequest,
    SnapshotRequestError,
)
from archiver.processors.export_ci_lake_snapshot import (
    export_ci_lake_snapshot as _export_ci_lake_snapshot,
)
from archiver.processors.flush_silver_observations import (
    flush_silver_observations as _flush_silver_observations,
)
from archiver.processors.flush_staging_events import flush_staging_events as _flush_staging_events
from archiver.processors.pack_bronze_html import pack_bronze_html as _pack_bronze_html
from archiver.processors.verify_pack_read_path import (
    verify_pack_read_path as _verify_pack_read_path,
)
from shared.api_envelope import ServiceFailure
from shared.api_models import (
    ErrorResponse,
    HealthResponse,
    NotReadyResponse,
    ReadyResponse,
)
from shared.job_counter import JobInFlight, active_job, job_snapshot, single_flight
from shared.logging_setup import configure_logging
from shared.minio import BUCKET as _MINIO_BUCKET

configure_logging()
logger = logging.getLogger("archiver")

app = FastAPI()

# source_base_path lets callers point selector/audit reads at a local fixture
# directory instead of s3://{MINIO_BUCKET}. That's needed for CLI/tests but
# must stay off by default on the HTTP route — it's an arbitrary local path
# interpolated into DuckDB read_parquet() calls.
_ALLOW_SOURCE_BASE_PATH = (
    os.environ.get("ARCHIVER_ALLOW_SOURCE_BASE_PATH", "false").lower() == "true"
)

# Plan 120 Gate C.5: production-sized cohort/export work (build_cohort=True)
# must not run synchronously inside the production archiver API process — a
# VM run showed it starves flush/cleanup/compact and Airflow health checks.
# That work belongs in the isolated snapshot-worker one-shot container (see
# docker-compose.yml). This flag exists only for tests/manual override.
_ALLOW_SYNC_SNAPSHOT_COHORT = (
    os.environ.get("ARCHIVER_ALLOW_SYNC_SNAPSHOT_COHORT", "false").lower() == "true"
)

# Plan 131 Stage 5 D4: month-scale pack/prune work has its own long-running
# service. Keeping the same endpoints on the regular archiver process would
# leave two live entry points and let a long job starve flush/cleanup/compact.
# Tests and deliberate manual use can override this on a process explicitly.
_ALLOW_PACK_JOBS = (
    os.environ.get("ARCHIVER_ALLOW_PACK_JOBS", "false").lower() == "true"
)

# ---------------------------------------------------------------------------
# Plan 134 — the flush/compact failure contract
# ---------------------------------------------------------------------------
#
# Exactly the gap the Plan 131 block below describes, on the three endpoints
# that block deliberately left alone. Each processor returns a summary rather
# than raising, this module returns it with a 200, and resp.raise_for_status()
# — the entire check a DAG performs — passes on a run that flushed nothing
# because the disk was full.
#
# Stage 0 measured what that has cost. A full MinIO produced 21 ERROR records
# over five days (2026-08-08 → 08-13); code deployed ahead of its migration
# produced 32 over sixteen hours (2026-08-26 → 08-27). Airflow recorded every
# one of the 128 covering runs as success, and dbt built on stale data through
# both. So these predicates are already known to be right on the two failure
# modes that have actually happened.
#
# What was not known is what *else* they would fire on, which is why Stage B
# warned instead of raising: an oversight cost a log line rather than a
# skipped dbt build every hour. That window closed clean on 2026-09-07 — zero
# warnings over 376 evaluations — so Stage C is flipping them to a 500, one
# endpoint per deploy and 48 hours apart, in ascending order of blast radius.
#
# **All three now raise** — /compact/silver/run, /flush/staging/run and
# /flush/silver/run, in that order. With the last one flipped nothing on this
# service warns and carries on, so the Stage B helper that emitted the
# ``would fail`` warning is gone. Nothing can emit that line any more, so a
# zero from its Loki query is not evidence of anything; read the ERROR-level
# ``<job>: run failed`` lines instead.
#
# The shape is _pack_failure_reason's, below: a pure function on the summary
# dict, mirroring that job's own CLI exit code, unit-tested directly against
# summary dicts, whose docstring records *why* each carried condition is
# carried — a predicate without that reasoning is how an hourly job gets
# failed over a quiet hour.


def _flush_silver_failure_reason(summary: Dict[str, Any]) -> Optional[str]:
    """Why this silver flush counts as failed, or None.

    ``error`` is the whole predicate. ``flush_silver_observations`` sets it on
    a DB connection failure, on a MinIO connection failure, and on any
    exception inside the write-then-delete transaction — the last of which is
    Stage 0's Incident 1, where PutObject returned XMinioStorageFull for five
    days while the DAG went green every hour.

    ``flushed == 0`` is **not** a failure. The processor returns
    ``{"flushed": 0, "error": None}`` when nothing is staged, which is the
    ordinary state of a quiet hour; failing on it would page forever on a
    system that is working.
    """
    if summary.get("error"):
        return f"flush aborted: {summary['error']}"
    return None


def _flush_staging_failure_reason(summary: Dict[str, Any]) -> Optional[str]:
    """Why this staging flush counts as failed, or None.

    The per-table failures are the condition worth naming. ``flush_staging_events``
    flushes each table independently and rolls any failure up into a top-level
    ``error`` of the literal ``"one or more tables failed"``, which says
    nothing about which — and Stage 0's Incident 2 was two tables out of six,
    missing for sixteen hours because the archiver image shipped ahead of V044
    and V045. A page that cannot name them sends a human to read six tables.

    The top-level ``error`` is still checked on its own, because the two
    connection-failure paths set it with an empty ``tables`` list and there is
    nothing per-table to name.

    ``total_flushed == 0`` is **not** a failure, for the same reason it is not
    on the silver flush: an hour with nothing staged is an ordinary hour.
    """
    failed = [
        table.get("table") or "?"
        for table in (summary.get("tables") or [])
        if table.get("error")
    ]
    if failed:
        return f"{len(failed)} staging table(s) failed to flush: {', '.join(failed)}"
    if summary.get("error"):
        return f"flush aborted: {summary['error']}"
    return None


def _compact_failure_reason(summary: Dict[str, Any]) -> Optional[str]:
    """Why this compaction run counts as failed, or None.

    **The predicate is ``error or failed``, and ``failed`` is the half that
    matters.** ``compact_silver`` catches per-partition exceptions, counts them
    in ``failed``, appends an ``{"ok": False, ...}`` entry to ``partitions``,
    and then returns the run summary with ``"error": None``. Its top-level
    ``error`` is set only when MinIO itself is unreachable, so a run in which
    every partition failed is ``{"failed": 7, "error": None}`` — and, before
    this predicate, a 200.

    A failed partition is not a soft signal here. ``_compact_one`` writes a
    ``.parquet.tmp``, verifies the row count, deletes the originals, then
    renames; a rename failure raises *after* the originals are gone, leaving an
    unpublished ``.tmp`` that the ``*.parquet`` glob does not match. That is
    data which is present and invisible to every reader until a human moves it,
    which is the condition on this service that most needs a person. So the
    reason names the offending partitions rather than only counting them, and
    the caller carries the whole summary — ``partitions`` entries included.

    ``skipped`` and ``incremental`` are **not** failures. A partition that is
    already compacted is skipped, an incremental merge is the normal daily
    path, and a run that is entirely skipped is a clean run.
    """
    if summary.get("error"):
        return f"run aborted: {summary['error']}"
    failed = summary.get("failed") or 0
    if failed:
        names = [
            f"{partition.get('source')}/{partition.get('date')}"
            for partition in (summary.get("partitions") or [])
            if not partition.get("ok", True)
        ]
        named = f": {', '.join(names)}" if names else ""
        return f"{failed} partition(s) failed to compact{named}"
    return None


@app.post("/cleanup/queue", response_model=CleanupResponse)
def run_cleanup_queue_batch(payload: dict = Body(...)) -> Dict[str, Any]:
    """Delete a caller-supplied list of artifacts_queue rows (status complete/skip)."""
    with active_job():
        artifact_ids = [int(i) for i in (payload or {}).get("artifact_ids", [])]
        results = _cleanup_queue(artifact_ids)
        deleted_count = sum(1 for r in results if r.get("deleted"))
        return {"total": len(results), "deleted": deleted_count,
                "failed": len(results) - deleted_count, "results": results}


@app.post("/cleanup/queue/run", response_model=CleanupResponse)
def trigger_cleanup_queue() -> Dict[str, Any]:
    """Sweep all complete/skip rows from artifacts_queue (Airflow DAG trigger)."""
    with active_job():
        return _run_cleanup_queue()


@app.post(
    "/flush/silver/run",
    response_model=FlushSilverResponse,
    responses={
        500: {
            "description": "The flush failed; the summary is the detail.",
            "model": ErrorResponse,
        },
    },
)
def trigger_flush_silver() -> Dict[str, Any]:
    """Flush staging.silver_observations to MinIO silver layer (Airflow DAG trigger).

    **Enforced.** A run failing ``_flush_silver_failure_reason`` returns 500
    with the summary and a ``failure_reason`` as ``detail``; both callers go
    through ``sensors.post_json``, which raises ``JsonPostError`` carrying that
    body, so the task goes red and ``hourly_analytics_refresh``'s ``notify``
    can quote the reason.

    Plan 134 Stage C, deploy 3 of 3 — last, deliberately. A red task here skips
    the hour's dbt build, as a red staging flush does, but this is the build
    that would otherwise have run on stale silver: Stage A's Incident 1 was five
    days of exactly that, green every hour.
    """
    with active_job():
        result = _flush_silver_observations()
        reason = _flush_silver_failure_reason(result)
        if reason:
            logger.error("flush_silver: run failed — %s", reason)
            raise ServiceFailure(detail=dict(result, failure_reason=reason))
        return result


@app.post(
    "/compact/silver/run",
    response_model=CompactSilverResponse,
    responses={
        500: {
            "description": "The run aborted; the summary is the detail.",
            "model": ErrorResponse,
        },
    },
)
def trigger_compact_silver() -> Dict[str, Any]:
    """Compact silver_normalized/observations partitions (Airflow DAG trigger).

    **Enforced.** A run failing ``_compact_failure_reason`` returns 500 with
    the summary and a ``failure_reason`` as ``detail``; ``compact_silver``
    calls ``raise_for_status()``, so the DAG goes red and pages.

    Plan 134 Stage C, deploy 1 of 3. First deliberately: this runs daily and
    nothing downstream depends on it, which makes it both the smallest blast
    radius and the cheapest place for the repaired pager to be wrong. The two
    flushes followed on their own deploys, 48 hours apart.
    """
    with active_job():
        result = _compact_silver()
        reason = _compact_failure_reason(result)
        if reason:
            logger.error("compact_silver: run failed — %s", reason)
            raise ServiceFailure(detail=dict(result, failure_reason=reason))
        return result


# ---------------------------------------------------------------------------
# Plan 131 — pack lifecycle (Stage 2 packing, Stage 4 pruning)
# ---------------------------------------------------------------------------
#
# Both processors return a summary dict rather than raising: partial results
# are still results, which is right for a job you run by hand and read. Both
# then translate that summary into a failure *on the CLI* — and the HTTP side
# never got the same translation, so resp.raise_for_status() passed on a run
# that deleted nothing, refused forty thousand objects, or never started.
#
# These two predicates give the HTTP side that contract, so a curl gets the
# same answer the DAG does. dbt_runner is the in-repo precedent: a 500 whose
# detail carries the whole result, which sensors.post_json parses back out and
# a notify task can quote.
#
# They start from each job's CLI exit code and diverge in one place: the packer
# exits 1 on read_failures and the endpoint only warns. A human running the CLI
# wants to know one object was unreadable; a monthly DAG must not throw away a
# packed month over it. The deleter's predicate is the CLI's exactly.
#
# Deliberately scoped to the two Plan 131 endpoints. flush/silver,
# flush/staging and compact/silver have exactly the same gap and should be
# fixed the same way — but that converts long-standing *silent* failures into
# sudden DAG failures and pages, which is its own change with its own blast
# radius, not something to smuggle in under a packing plan.
#
# That change is Plan 134, and its predicates are in the block above. They are
# warning-only through Stage 1's observation window; these three have raised
# since Plan 131 Stage 5.


@contextmanager
def _single_flight_or_409(job: str):
    """Hold *job*'s single-flight slot, or 409 if something else has it.

    There is no lock on either endpoint without this, which was fine while
    every run was a human typing a command. It stops being fine the moment a
    DAG retries: a multi-hour HTTP call that dies on a dropped connection
    leaves the job **still running**, and the retry would start a second packer
    on the same bucket — two runs computing the same ``next_seq`` and racing to
    write packs under the same key.

    See ``shared.job_counter.single_flight`` for what this does and does not
    cover; in particular it does not see a manual CLI run in another process.
    """
    try:
        with single_flight(job):
            yield
    except JobInFlight:
        logger.warning("%s: refused — a run is already in flight", job)
        raise HTTPException(
            status_code=409,
            detail=f"{job} is already in flight on this service; skipping.",
        )


def _pack_failure_reason(summary: Dict[str, Any]) -> Optional[str]:
    """Why this pack run counts as failed, or None.

    Only an aborted run. Everything else warns and carries:

    - ``read_failures`` — a source object could not be read, so it was left
      unpacked and its bytes are still exactly where they were. Packing is
      additive and nothing is deleted here, so an unreadable object costs a
      later run, not any data. The packer's CLI exits 1 on this
      (``pack_bronze_html.py:996``) and the endpoint deliberately does not: a
      monthly DAG that fails on one bad object out of 557,065 abandons the
      other 557,064. Each one is already logged at WARNING by the packer.
    - ``stopped_at_max_packs`` — the cap doing its job.
    - ``orphan_packs`` — an earlier run was interrupted. The packer reports
      orphans and never writes into them, so carrying the condition is safe.
    """
    if summary.get("error"):
        return f"run aborted: {summary['error']}"
    return None


def _prune_failure_reason(summary: Dict[str, Any]) -> Optional[str]:
    """Why this prune run counts as failed, or None. Mirrors the CLI's exit 1.

    ``objects_refused`` is the loudest signal this job produces: one of the
    three per-member checks disagreed, so a pack or the resolver is wrong.
    Nothing was lost — that is the safety property working — but it must not
    return 200.

    ``objects_deleted == 0`` is **not** a failure on its own: a fully drained
    month legitimately deletes nothing and returns after one listing. Neither
    is ``capped``.
    """
    if summary.get("error"):
        return f"run aborted: {summary['error']}"
    refused = summary.get("objects_refused") or 0
    if refused:
        return (
            f"{refused} object(s) refused verification; nothing was deleted for them"
        )
    return None


def _verify_failure_reason(summary: Dict[str, Any]) -> Optional[str]:
    """Why this read-path verification counts as failed, or None.

    This is the verifier CLI's exit predicate exactly: any failed member or no
    verified members is a failure. The endpoint carries the full summary in its
    500 response so a scheduled canary can report the offending keys.
    """
    failed = summary.get("failed") or 0
    if failed:
        return f"{failed} sampled member(s) failed read-path verification"
    if not (summary.get("verified") or 0):
        return "no sampled members were verified"
    return None


def _require_pack_worker() -> None:
    """Refuse month-scale mutation jobs on the regular archiver service."""
    if _ALLOW_PACK_JOBS:
        return
    raise HTTPException(
        status_code=409,
        detail=(
            "Pack and prune jobs are disabled on the production archiver API. "
            "Run them on pack-worker at http://pack-worker:8001; a month-scale "
            "run starves flush/cleanup/compact here. Set "
            "ARCHIVER_ALLOW_PACK_JOBS=true to override for tests or manual use."
        ),
    )


@app.post(
    "/pack/bronze/run",
    response_model=PackBronzeResponse,
    responses={
        400: {
            "description": "The request arguments are not usable.",
            "model": ErrorResponse,
        },
        409: {
            "description": "Another run of this job is already in flight.",
            "model": ErrorResponse,
        },
        500: {
            "description": "The run aborted; the summary is the detail.",
            "model": ErrorResponse,
        },
    },
)
def trigger_pack_bronze_html(payload: dict = Body(default={})) -> Dict[str, Any]:
    """Pack cold bronze HTML into indexed packs (Plan 131 Stage 2).

    Dry-run unless the caller passes ``apply: true``. Stage 2 writes packs
    alongside their sources and deletes nothing — deleting packed sources is
    Stage 4 and has its own endpoint when it exists.

    Returns **500** with the summary as ``detail`` when the run aborted.
    ``read_failures`` warn and carry — see ``_pack_failure_reason``.

    Returns **409** while another pack run is in flight (D3a), which
    ``sensors.post_json`` turns into a graceful skip.
    """
    _require_pack_worker()
    with active_job(), _single_flight_or_409("pack_bronze"):
        payload = payload or {}
        try:
            kwargs = {
                key: payload[key]
                for key in (
                    "apply", "artifact_type", "year", "month", "max_buckets",
                    "max_packs", "max_pack_bytes", "frame_target_bytes",
                    "settle_days", "min_free_bytes", "dict_id",
                    "allow_no_dictionary",
                )
                if key in payload
            }
            result = _pack_bronze_html(**kwargs)
        except (ValueError, TypeError) as e:
            raise HTTPException(status_code=400, detail=f"Invalid request payload: {e}")

        reason = _pack_failure_reason(result)
        if reason:
            logger.error("pack_bronze_html: run failed — %s", reason)
            raise HTTPException(
                status_code=500, detail=dict(result, failure_reason=reason)
            )
        if result.get("read_failures"):
            # Returning 200 on this is a decision, so it says so once here
            # rather than only in the per-object warnings the packer emits.
            logger.warning(
                "pack_bronze_html: %d source object(s) could not be read and were "
                "left unpacked; a later run will pick them up",
                result["read_failures"],
            )
        return result


@app.post(
    "/pack/bronze/prune",
    response_model=PrunePackedResponse,
    responses={
        400: {
            "description": "The request arguments are not usable.",
            "model": ErrorResponse,
        },
        409: {
            "description": "Another run of this job is already in flight.",
            "model": ErrorResponse,
        },
        500: {
            "description": "The run aborted; the summary is the detail.",
            "model": ErrorResponse,
        },
    },
)
def trigger_prune_packed_source_html(payload: dict = Body(default={})) -> Dict[str, Any]:
    """Delete bronze HTML objects that are inside a verified pack (Plan 131 Stage 4).

    **Dry-run unless the caller passes ``apply: true``**, and capped by
    ``max_objects`` / ``max_packs`` in either mode. This is the only endpoint on
    this service that removes bronze data; the bucket is un-versioned, so a
    delete is immediate and there is no undo.

    ``year`` and ``month`` are required. Deliberately: the packer can discover
    what is eligible because packing is additive, and this cannot, because it
    is not.

    Returns **500** with the summary as ``detail`` when the run aborted or
    refused an object, matching the CLI's exit code — see
    ``_prune_failure_reason``.

    Returns **409** while another prune run is in flight (D3a). Packing is a
    separate key, so a pack and a prune may run at once.
    """
    _require_pack_worker()
    with active_job(), _single_flight_or_409("pack_prune"):
        payload = payload or {}
        if "year" not in payload or "month" not in payload:
            raise HTTPException(
                status_code=400,
                detail="year and month are required — this endpoint deletes data",
            )
        try:
            kwargs = {
                key: payload[key]
                for key in (
                    "apply", "artifact_type", "year", "month", "max_objects",
                    "max_packs", "grace_days", "sample_full_reads", "status_breakdown",
                )
                if key in payload
            }
            result = _delete_packed_source_html(**kwargs)
        except (ValueError, TypeError) as e:
            raise HTTPException(status_code=400, detail=f"Invalid request payload: {e}")

        reason = _prune_failure_reason(result)
        if reason:
            logger.error("delete_packed_source_html: run failed — %s", reason)
            raise HTTPException(
                status_code=500, detail=dict(result, failure_reason=reason)
            )
        return result


@app.post(
    "/pack/bronze/verify",
    response_model=VerifyPackResponse,
    responses={
        400: {
            "description": "The request arguments are not usable.",
            "model": ErrorResponse,
        },
        500: {
            "description": "The run aborted; the summary is the detail.",
            "model": ErrorResponse,
        },
    },
)
def trigger_verify_pack_read_path(payload: dict = Body(default={})) -> Dict[str, Any]:
    """Sample a packed month through the production read path (read-only).

    Unlike pack and prune this endpoint is intentionally outside their
    single-flight slots and the pack-worker guard. A canary must stay available
    while either mutation job is running, which is when it is most useful.
    """
    with active_job():
        payload = payload or {}
        if "year" not in payload or "month" not in payload:
            raise HTTPException(
                status_code=400,
                detail="year and month are required for pack read-path verification",
            )
        try:
            kwargs = {
                "artifact_type": payload.get("artifact_type", "detail_page"),
                "year": payload["year"],
                "month": payload["month"],
                "per_pack": payload.get("per_pack", 5),
                "warm_reads": payload.get("warm_reads", 50),
                "seed": payload.get("seed", 131),
                "bucket": _MINIO_BUCKET,
            }
            result = _verify_pack_read_path(**kwargs)
        except (ValueError, TypeError) as e:
            raise HTTPException(status_code=400, detail=f"Invalid request payload: {e}")

        reason = _verify_failure_reason(result)
        if reason:
            logger.error("verify_pack_read_path: run failed — %s", reason)
            raise HTTPException(
                status_code=500, detail=dict(result, failure_reason=reason)
            )
        return result


@app.post(
    "/flush/staging/run",
    response_model=FlushStagingResponse,
    responses={
        500: {
            "description": "The flush failed; the summary is the detail.",
            "model": ErrorResponse,
        },
    },
)
def trigger_flush_staging() -> Dict[str, Any]:
    """Flush all staging event tables to MinIO Parquet (Airflow DAG trigger).

    **Enforced.** A run failing ``_flush_staging_failure_reason`` returns 500
    with the summary and a ``failure_reason`` as ``detail`` naming the tables
    that did not land; both callers go through ``sensors.post_json``, which
    raises ``JsonPostError`` carrying that body, so the task goes red and
    ``hourly_analytics_refresh``'s ``notify`` can quote the reason.

    Plan 134 Stage C, deploy 2 of 3. Second because the dbt build does not read
    staging events, so the hour dbt skips here is an hour it would have built
    from unchanged inputs anyway — which is *not* the same as the build being
    unaffected: ``flush_staging_events`` sits upstream of ``dbt_build`` in the
    DAG, so a red task here skips it. ``/flush/silver/run`` went last, on
    deploy 3, because there the skipped build would also have been building on
    stale data.
    """
    with active_job():
        result = _flush_staging_events()
        reason = _flush_staging_failure_reason(result)
        if reason:
            logger.error("flush_staging: run failed — %s", reason)
            raise ServiceFailure(detail=dict(result, failure_reason=reason))
        return result


def _require_disk_usage_host_mounts() -> None:
    """Refuse the disk walk on a service that cannot see the disks.

    Only pack-worker carries the read-only host mounts and the writable
    textfile volume. Without them every measurement fails and the job would
    still cheerfully write a .prom with nothing in it -- which reads as
    "the disks are empty" rather than as an error.
    """
    if _disk_usage_textfile_dir():
        return
    raise HTTPException(
        status_code=409,
        detail=(
            "Disk usage measurement is not configured on this service. It runs "
            "on pack-worker at http://pack-worker:8001, which carries the "
            "read-only host mounts and the node-exporter textfile volume. Set "
            "DISK_USAGE_TEXTFILE_DIR to enable it elsewhere."
        ),
    )


@app.post(
    "/disk-usage/run",
    response_model=DiskUsageResponse,
    responses={
        409: {
            "description": "Another run of this job is already in flight.",
            "model": ErrorResponse,
        },
        500: {
            "description": "The run aborted; the summary is the detail.",
            "model": ErrorResponse,
        },
    },
)
def trigger_disk_usage(payload: dict = Body(default={})) -> Dict[str, Any]:
    """Measure the disk watchlist and publish it via node-exporter (Plan 135 Stage 4).

    ``include_slow: true`` adds the high-inode volumes (MinIO bronze, Airflow
    task logs), which between them take 20+ minutes and belong on the weekly
    run only. Everything else is walked on every call and the slow-tier values
    are carried forward in between.

    Returns **500** when a scheduled measurement failed, so a silently empty
    band on the dashboard cannot pass as a successful run.
    """
    _require_disk_usage_host_mounts()
    with active_job():
        payload = payload or {}
        result = _run_disk_usage(include_slow=bool(payload.get("include_slow", False)))
        if result["failed"]:
            logger.error(
                "disk_usage: %d watchlist target(s) could not be measured: %s",
                result["failed"], result["unpublished"],
            )
            raise HTTPException(status_code=500, detail=result)
        return result


@app.post(
    "/snapshots/adaptive-refresh/run",
    response_model=SnapshotExportResponse,
    responses={
        400: {
            "description": "The requested tier is not one this service exports.",
            "model": ErrorResponse,
        },
        409: {
            "description": "Another run of this job is already in flight.",
            "model": ErrorResponse,
        },
    },
)
def trigger_snapshot_export(payload: dict = Body(default={})) -> Dict[str, Any]:
    """Generate (or dry-run plan) a CI lake snapshot (Plan 120)."""
    with active_job():
        payload = payload or {}
        window_start = payload.get("source_window_start")
        window_end = payload.get("source_window_end")
        source_base_path = payload.get("source_base_path")
        if source_base_path is not None and not _ALLOW_SOURCE_BASE_PATH:
            raise HTTPException(
                status_code=400,
                detail="source_base_path is not permitted on this endpoint",
            )
        # A non-dry-run request also always runs full selector/cohort
        # planning (Gate D — a real export needs a closed cohort regardless
        # of the build_cohort flag), so it must be guarded exactly like
        # build_cohort=True; otherwise a plain dry_run=False request would
        # slip past this check and run production-sized work synchronously.
        # audit_sources is exempt either way — it never runs selector/cohort
        # planning, dry-run or not.
        requires_sync_cohort_guard = (
            not payload.get("audit_sources", False)
            and (payload.get("build_cohort", False) or not payload.get("dry_run", False))
        )
        if requires_sync_cohort_guard and not _ALLOW_SYNC_SNAPSHOT_COHORT:
            raise HTTPException(
                status_code=409,
                detail=(
                    "build_cohort/non-dry-run export is disabled on the production "
                    "archiver API. "
                    "Production-sized cohort/export work must run in the isolated "
                    "snapshot-worker container, e.g.: docker compose run --rm "
                    "snapshot-worker python -m archiver.processors."
                    "export_ci_lake_snapshot --tier edge --dry-run --run-selectors "
                    "--build-cohort --source-window-months 1 --target-vins 100. "
                    "Set ARCHIVER_ALLOW_SYNC_SNAPSHOT_COHORT=true to override for "
                    "tests or manual use."
                ),
            )
        try:
            request = SnapshotRequest(
                tier=payload.get("tier"),
                snapshot_id=payload.get("snapshot_id"),
                target_vins=payload.get("target_vins"),
                max_archive_mb=payload.get("max_archive_mb"),
                max_rows=payload.get("max_rows"),
                source_window_start=datetime.fromisoformat(window_start) if window_start else None,
                source_window_end=datetime.fromisoformat(window_end) if window_end else None,
                source_window_months=payload.get("source_window_months"),
                require_selector_coverage=payload.get("require_selector_coverage", False),
                dry_run=payload.get("dry_run", False),
                audit_sources=payload.get("audit_sources", False),
                run_selectors=payload.get("run_selectors", False),
                build_cohort=payload.get("build_cohort", False),
                source_base_path=source_base_path,
                reuse_planning_cache=payload.get("reuse_planning_cache", False),
                refresh_planning_cache=payload.get("refresh_planning_cache", False),
                planning_cache_bucket_grain=payload.get(
                    "planning_cache_bucket_grain", "week"
                ),
                planning_cache_prefix=payload.get(
                    "planning_cache_prefix", "snapshot_planning_cache"
                ),
            )
            result = _export_ci_lake_snapshot(request)
        except SnapshotRequestError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except (ValueError, TypeError) as e:
            raise HTTPException(status_code=400, detail=f"Invalid request payload: {e}")
        return result.to_dict()


@app.get("/health", response_model=HealthResponse)
def health():
    return {"ok": True}


@app.get(
    "/ready",
    response_model=ReadyResponse,
    responses={
        503: {
            "description": "A dependency this service needs is not reachable.",
            "model": NotReadyResponse,
        },
    },
)
def ready():
    evidence = job_snapshot()
    result = {"ready": evidence["active_jobs"] == 0, **evidence}
    if result["ready"]:
        return result
    raise HTTPException(status_code=503, detail={**result, "reason": "jobs in flight"})
