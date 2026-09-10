"""
Processing service — artifact parsing and observation writes for cartracker.

Slim entrypoint: health/ready endpoints plus router includes.
All processing logic lives in routers/ and writers/.
"""
from fastapi import FastAPI, HTTPException, Response
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, generate_latest
from prometheus_fastapi_instrumentator import Instrumentator

from processing.routers.artifact import router as artifact_router
from processing.routers.batch import router as batch_router
from shared.api_models import HealthResponse, NotReadyResponse, ReadyResponse
from shared.job_counter import job_snapshot
from shared.logging_setup import configure_logging

configure_logging()

app = FastAPI()
Instrumentator().instrument(app)

# Plan 162 Stage AA: `/metrics` is declared here rather than by
# `Instrumentator.expose()`, which registers a closure from the library. That
# closure is not a function in this repository, so no rule that resolves a
# route to its handler could reach this route -- and a rule that cannot reach a
# route is a rule with a hole in it rather than a rule with an exception.
#
# Equivalent to what `expose()` served: its only other branch is the
# multiprocess registry, and `PROMETHEUS_MULTIPROC_DIR` is set nowhere in this
# repository and unset in all three running containers. `should_gzip` was left
# at its default of False. `instrument()` still does the collecting.
@app.get("/metrics", response_class=Response)
def metrics() -> Response:
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)



app.include_router(batch_router)
app.include_router(artifact_router)


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
    """
    Drain signal for Plan 92 / Airflow.

    Returns ready=true when no batch is currently executing.
    Airflow sensors poll this before closing a DAG run.
    """
    evidence = job_snapshot()
    result = {"ready": evidence["active_jobs"] == 0, **evidence}
    if result["ready"]:
        return result
    raise HTTPException(
        status_code=503,
        detail={**result, "reason": "batch in progress"},
    )
