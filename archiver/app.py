import logging
import os
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import Body, FastAPI, HTTPException

from archiver.processors.cleanup_parquet import cleanup_parquet as _cleanup_parquet
from archiver.processors.cleanup_parquet import run_cleanup_parquet as _run_cleanup_parquet
from archiver.processors.cleanup_queue import cleanup_queue as _cleanup_queue
from archiver.processors.cleanup_queue import run_cleanup_queue as _run_cleanup_queue
from archiver.processors.compact_silver import compact_silver as _compact_silver
from archiver.processors.delete_packed_source_html import (
    delete_packed_source_html as _delete_packed_source_html,
)
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
from shared.job_counter import JobInFlight, active_job, is_idle, single_flight
from shared.logging_setup import configure_logging

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


@app.post("/cleanup/parquet")
def run_cleanup_parquet(payload: dict = Body(...)) -> Dict[str, Any]:
    with active_job():
        paths = (payload or {}).get("paths", [])
        results = _cleanup_parquet(paths)
        deleted_count = sum(1 for r in results if r.get("deleted"))
        return {"total": len(results), "deleted": deleted_count,
                "failed": len(results) - deleted_count, "results": results}


@app.post("/cleanup/parquet/run")
def trigger_cleanup_parquet() -> Dict[str, Any]:
    with active_job():
        return _run_cleanup_parquet()


@app.post("/cleanup/queue")
def run_cleanup_queue_batch(payload: dict = Body(...)) -> Dict[str, Any]:
    """Delete a caller-supplied list of artifacts_queue rows (status complete/skip)."""
    with active_job():
        artifact_ids = [int(i) for i in (payload or {}).get("artifact_ids", [])]
        results = _cleanup_queue(artifact_ids)
        deleted_count = sum(1 for r in results if r.get("deleted"))
        return {"total": len(results), "deleted": deleted_count,
                "failed": len(results) - deleted_count, "results": results}


@app.post("/cleanup/queue/run")
def trigger_cleanup_queue() -> Dict[str, Any]:
    """Sweep all complete/skip rows from artifacts_queue (Airflow DAG trigger)."""
    with active_job():
        return _run_cleanup_queue()


@app.post("/flush/silver/run")
def trigger_flush_silver() -> Dict[str, Any]:
    """Flush staging.silver_observations to MinIO silver layer (Airflow DAG trigger)."""
    with active_job():
        return _flush_silver_observations()


@app.post("/compact/silver/run")
def trigger_compact_silver() -> Dict[str, Any]:
    """Compact silver_normalized/observations partitions (Airflow DAG trigger)."""
    with active_job():
        return _compact_silver()


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


@app.post("/pack/bronze/run")
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


@app.post("/pack/bronze/prune")
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


@app.post("/flush/staging/run")
def trigger_flush_staging() -> Dict[str, Any]:
    """Flush all staging event tables to MinIO Parquet (Airflow DAG trigger)."""
    with active_job():
        return _flush_staging_events()


@app.post("/snapshots/adaptive-refresh/run")
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


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/ready")
def ready():
    if is_idle():
        return {"ready": True}
    raise HTTPException(status_code=503, detail={"ready": False, "reason": "jobs in flight"})
