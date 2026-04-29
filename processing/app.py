"""
Processing service — artifact parsing and observation writes for cartracker.

Slim entrypoint: health/ready endpoints plus router includes.
All processing logic lives in routers/ and writers/.
"""
import logging
import os
from logging.handlers import RotatingFileHandler

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from processing.routers.artifact import router as artifact_router
from processing.routers.batch import router as batch_router
from shared.job_counter import is_idle

_LOG_PATH = os.getenv("LOG_PATH", "/usr/app/logs/app.log")
os.makedirs(os.path.dirname(_LOG_PATH), exist_ok=True)
_log_handler = RotatingFileHandler(_LOG_PATH, maxBytes=5_000_000, backupCount=3)
_log_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
logging.getLogger().addHandler(_log_handler)
logging.getLogger().setLevel(logging.INFO)

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
    idle = is_idle()
    if idle:
        return {"ready": True}
    return {"ready": False, "reason": "batch in progress"}
