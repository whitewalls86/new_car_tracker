"""
Processing service — artifact parsing and observation writes for cartracker.

Slim entrypoint: health/ready endpoints plus router includes.
All processing logic lives in routers/ and writers/.
"""
from fastapi import FastAPI, HTTPException
from prometheus_fastapi_instrumentator import Instrumentator

from processing.routers.artifact import router as artifact_router
from processing.routers.batch import router as batch_router
from shared.job_counter import is_idle
from shared.logging_setup import configure_logging

configure_logging()

app = FastAPI()
Instrumentator().instrument(app).expose(app)

app.include_router(batch_router)
app.include_router(artifact_router)


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/ready")
def ready():
    """
    Drain signal for Plan 92 / Airflow.

    Returns ready=true when no batch is currently executing.
    Airflow sensors poll this before closing a DAG run.
    """
    if is_idle():
        return {"ready": True}
    raise HTTPException(status_code=503, detail={"ready": False, "reason": "batch in progress"})
