"""Fail-closed stack release evidence for Plan 142 Stage 3."""

import os
import socket
import time
from collections.abc import Callable
from math import isfinite
from typing import Any

import requests

from container_health.expected import EXPECTED_SERVICES

HTTP_TIMEOUT_SECONDS = 3
PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090")
LOKI_URL = os.environ.get("LOKI_URL", "http://loki:3100")
CONTAINER_HEALTH_URL = os.environ.get(
    "CONTAINER_HEALTH_URL", "http://container-health:9110"
)

# These are direct, dependency-appropriate readiness checks.  Postgres has no
# HTTP readiness endpoint, so its direct probe is the database TCP listener.
READINESS_TARGETS = {
    "postgres": ("tcp", "postgres", 5432),
    "minio": ("http", "http://minio:9000/minio/health/live", None),
    "airflow": ("http", "http://airflow-apiserver:8080/api/v2/monitor/health", None),
    "ops": ("http", "http://ops:8060/health", None),
    "prometheus": ("http", "http://prometheus:9090/-/ready", None),
    "grafana": ("http", "http://grafana:3000/api/health", None),
    "loki": ("http", "http://loki:3100/ready", None),
    "promtail": ("http", "http://promtail:9080/ready", None),
}
AUXILIARY_PROJECTS = frozenset({"cartracker-lakehouse", "cartracker-mlflow"})
MAX_SCRAPE_AGE_SECONDS = 60
LOG_INGESTION_LOOKBACK_SECONDS = 600


def _passed(gate: str, reason: str = "") -> dict[str, str]:
    result = {"gate": gate, "status": "pass"}
    if reason:
        result["reason"] = reason
    return result


def _failed(gate: str, reason: str) -> dict[str, str]:
    return {"gate": gate, "status": "fail", "reason": reason}


def _unknown(gate: str, reason: str) -> dict[str, str]:
    return {"gate": gate, "status": "unknown", "reason": reason}


def _prometheus_scalar(query: str) -> float:
    response = requests.get(
        f"{PROMETHEUS_URL.rstrip('/')}/api/v1/query",
        params={"query": query},
        timeout=HTTP_TIMEOUT_SECONDS,
    )
    payload = response.json()
    result = payload["data"]["result"]
    if payload.get("status") != "success" or len(result) != 1:
        raise ValueError("Prometheus returned no scalar result")
    value = result[0]["value"][1]
    scalar = float(value)
    if not isfinite(scalar):
        raise ValueError("Prometheus returned a non-finite scalar")
    return scalar


def _container_health_values() -> dict[str, int]:
    response = requests.get(
        f"{PROMETHEUS_URL.rstrip('/')}/api/v1/query",
        params={"query": "cartracker_container_health"},
        timeout=HTTP_TIMEOUT_SECONDS,
    )
    payload = response.json()
    result = payload["data"]["result"]
    if payload.get("status") != "success" or not isinstance(result, list):
        raise ValueError("container-health metric unavailable")
    values = {}
    for item in result:
        value = float(item["value"][1])
        if value not in {-1, 0, 1}:
            raise ValueError("invalid container-health value")
        values[item["metric"]["container"]] = int(value)
    if not values:
        raise ValueError("container-health metric was empty")
    return values


def _expected_services_present(_: dict[str, Any]) -> dict[str, str]:
    gate = "expected_services_present"
    try:
        values = _container_health_values()
    except (requests.RequestException, KeyError, TypeError, ValueError, OverflowError):
        return _unknown(gate, "container-health evidence unavailable or malformed")
    missing = sorted(EXPECTED_SERVICES - values.keys())
    if missing:
        return _failed(gate, f"expected services absent: {', '.join(missing)}")
    return _passed(gate)


def _container_health(_: dict[str, Any]) -> dict[str, str]:
    gate = "container_health"
    try:
        values = _container_health_values()
    except (requests.RequestException, KeyError, TypeError, ValueError, OverflowError):
        return _unknown(gate, "container-health evidence unavailable or malformed")
    bad = sorted(service for service in EXPECTED_SERVICES if values.get(service) != 1)
    if bad:
        return _failed(gate, f"unhealthy, unconfigured, or absent: {', '.join(bad)}")
    return _passed(gate)


def _ready(target: tuple[str, str, int | None]) -> bool:
    protocol, address, port = target
    if protocol == "tcp":
        with socket.create_connection((address, port), timeout=HTTP_TIMEOUT_SECONDS):
            return True
    return requests.get(address, timeout=HTTP_TIMEOUT_SECONDS).status_code == 200


def _service_readiness(_: dict[str, Any]) -> dict[str, str]:
    gate = "service_readiness"
    unavailable = []
    for service, target in READINESS_TARGETS.items():
        try:
            if not _ready(target):
                unavailable.append(service)
        except (OSError, requests.RequestException):
            unavailable.append(service)
    if unavailable:
        return _failed(gate, f"readiness failed: {', '.join(unavailable)}")
    return _passed(gate)


def _loki_has_recent_ingestion() -> bool:
    response = requests.get(
        f"{LOKI_URL.rstrip('/')}/loki/api/v1/query_range",
        params={"query": '{source=~".+"}', "limit": 1, "direction": "BACKWARD"},
        timeout=HTTP_TIMEOUT_SECONDS,
    )
    payload = response.json()
    values = payload["data"]["result"]
    if payload.get("status") != "success" or not values:
        return False
    timestamp_ns = int(values[0]["values"][0][0])
    age_seconds = time.time() - (timestamp_ns / 1_000_000_000)
    return 0 <= age_seconds <= LOG_INGESTION_LOOKBACK_SECONDS


def _observability_fresh(_: dict[str, Any]) -> dict[str, str]:
    gate = "observability_fresh"
    try:
        scrape_age = _prometheus_scalar("max(time() - timestamp(up))")
        promtail_errors = _prometheus_scalar(
            "sum(increase(promtail_client_request_errors_total[5m]))"
        )
        ingested = _loki_has_recent_ingestion()
    except (requests.RequestException, KeyError, TypeError, ValueError, IndexError, OverflowError):
        return _unknown(gate, "observability evidence unavailable or malformed")
    if scrape_age > MAX_SCRAPE_AGE_SECONDS:
        return _failed(gate, f"Prometheus scrape age {scrape_age:.0f}s exceeds limit")
    if promtail_errors > 0:
        return _failed(gate, "Promtail client error storm detected")
    if not ingested:
        return _failed(gate, "Loki has no recent Promtail ingestion")
    return _passed(gate)


def _auxiliary_still_stopped(_: dict[str, Any]) -> dict[str, str]:
    gate = "auxiliary_still_stopped"
    running = []
    try:
        for project in sorted(AUXILIARY_PROJECTS):
            response = requests.get(
                f"{CONTAINER_HEALTH_URL.rstrip('/')}/project-status/{project}",
                timeout=HTTP_TIMEOUT_SECONDS,
            )
            payload = response.json()
            if payload.get("known") is not True or not isinstance(payload.get("services"), list):
                raise ValueError("invalid auxiliary project evidence")
            running.extend(f"{project}/{service}" for service in payload["services"])
    except (requests.RequestException, KeyError, TypeError, ValueError, IndexError, OverflowError):
        return _unknown(gate, "auxiliary project evidence unavailable or malformed")
    if running:
        return _failed(gate, f"auxiliary services running: {', '.join(running)}")
    return _passed(gate)


def _coordination_expected(state: dict[str, Any]) -> dict[str, str]:
    gate = "coordination_expected"
    if state.get("phase") != "validating" or state.get("kind") != "host_maintenance":
        return _failed(gate, "coordination is not validating host maintenance")
    return _passed(gate)


RELEASE_GATES: dict[str, Callable[[dict[str, Any]], dict[str, str]]] = {
    "expected_services_present": _expected_services_present,
    "container_health": _container_health,
    "service_readiness": _service_readiness,
    "observability_fresh": _observability_fresh,
    "auxiliary_still_stopped": _auxiliary_still_stopped,
    "coordination_expected": _coordination_expected,
}


def collect_release_status(state: dict[str, Any]) -> dict[str, Any]:
    """Evaluate every stack gate; unavailable evidence never grants release."""
    gates = [reader(state) for reader in RELEASE_GATES.values()]
    blockers = [gate["gate"] for gate in gates if gate["status"] != "pass"]
    return {
        "phase": state.get("phase"),
        "kind": state.get("kind"),
        "release_ready": not blockers,
        "blockers": blockers,
        "gates": gates,
    }
