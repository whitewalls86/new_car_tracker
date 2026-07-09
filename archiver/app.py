import logging
import os
from datetime import datetime
from typing import Any, Dict

from fastapi import Body, FastAPI, HTTPException

from archiver.processors.cleanup_parquet import cleanup_parquet as _cleanup_parquet
from archiver.processors.cleanup_parquet import run_cleanup_parquet as _run_cleanup_parquet
from archiver.processors.cleanup_queue import cleanup_queue as _cleanup_queue
from archiver.processors.cleanup_queue import run_cleanup_queue as _run_cleanup_queue
from archiver.processors.compact_silver import compact_silver as _compact_silver
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
from shared.job_counter import active_job, is_idle
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
