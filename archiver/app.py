import logging
from typing import Any, Dict

from fastapi import Body, FastAPI, HTTPException

from archiver.processors.cleanup_parquet import cleanup_parquet as _cleanup_parquet
from archiver.processors.cleanup_parquet import run_cleanup_parquet as _run_cleanup_parquet
from archiver.processors.cleanup_queue import cleanup_queue as _cleanup_queue
from archiver.processors.cleanup_queue import run_cleanup_queue as _run_cleanup_queue
from archiver.processors.flush_silver_observations import (
    flush_silver_observations as _flush_silver_observations,
)
from archiver.processors.flush_staging_events import flush_staging_events as _flush_staging_events
from shared.job_counter import active_job, is_idle
from shared.logging_setup import configure_logging

configure_logging()
logger = logging.getLogger("archiver")

app = FastAPI()


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


@app.post("/flush/staging/run")
def trigger_flush_staging() -> Dict[str, Any]:
    """Flush all staging event tables to MinIO Parquet (Airflow DAG trigger)."""
    with active_job():
        return _flush_staging_events()


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/ready")
def ready():
    if is_idle():
        return {"ready": True}
    raise HTTPException(status_code=503, detail={"ready": False, "reason": "jobs in flight"})
