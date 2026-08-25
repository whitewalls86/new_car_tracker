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

from container_health.collector import ContainerHealthCollector, oneoff_processes
from container_health.docker_api import DockerApi
from container_health.expected import EXPECTED_SERVICES

DOCKER_API_URL = os.environ.get("DOCKER_API_URL", "http://docker-socket-proxy:2375")
COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT", "cartracker")

# Deliberately not an environment variable. The expected set is resolved from
# Plan 142's manifest at build time and asserted in CI; reading it from the
# environment would put a hand-maintained service list in docker-compose.yml,
# where nothing checks it, and would break the test that asserts this container
# holds exactly two configuration values.
REGISTRY = CollectorRegistry()
REGISTRY.register(
    ContainerHealthCollector(
        DockerApi(DOCKER_API_URL), COMPOSE_PROJECT, EXPECTED_SERVICES
    )
)

app = FastAPI()
DOCKER_API = DockerApi(DOCKER_API_URL)


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


@app.get("/oneoff-processes")
def active_oneoff_processes() -> Dict[str, Any]:
    """Expose read-only live execution evidence for Plan 142 drain aggregation."""
    processes = oneoff_processes(
        DOCKER_API.inspect_project_containers(COMPOSE_PROJECT), COMPOSE_PROJECT
    )
    return {"known": True, "active_processes": len(processes), "processes": processes}
