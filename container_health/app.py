"""Container-health exporter (Plan 140 Stage 2).

A dedicated service with no database, object-store, or Telegram credentials.
The Docker grant it does hold is read-only and lands nowhere near the ones
`pack-worker` carries.
"""
from __future__ import annotations

import os
from typing import Any, Dict

from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest

from container_health.collector import ContainerHealthCollector
from container_health.docker_api import DockerApi

DOCKER_API_URL = os.environ.get("DOCKER_API_URL", "http://docker-socket-proxy:2375")
COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT", "cartracker")

REGISTRY = CollectorRegistry()
REGISTRY.register(ContainerHealthCollector(DockerApi(DOCKER_API_URL), COMPOSE_PROJECT))

app = FastAPI()


@app.get("/health")
def health() -> Dict[str, Any]:
    """Deliberately shallow: process liveness, never dependency health.

    Probing Docker from here would make this container's own health a function
    of the proxy's, and a cascading healthcheck is a worse signal than none.
    The exporter's real liveness is `up{job="container-health"}` -- if the
    Docker read fails, /metrics returns 500 and `ct-service-down` fires.
    """
    return {"ok": True}


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)
