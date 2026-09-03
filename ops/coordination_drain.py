"""Scoped, fail-closed drain evidence aggregation for Plan 142."""

import os
from datetime import datetime
from typing import Any, Callable

import requests

from airflow.dags.coordination_contract import ADMISSION_SURFACES, DRAIN_TASKS
from ops.coordination_contract import SERVICE_CONTRACTS
from ops.mutation_contract import DRAIN_SOURCES, required_drain_sources
from ops.queries import (
    SELECT_AIRFLOW_GATE_OBSERVATIONS,
    SELECT_AIRFLOW_TASK_INSTANCES,
    SELECT_PROCESSING_ARTIFACTS_BACKLOG,
    SELECT_RUNNING_DETAIL_CLAIMS,
)
from shared.db import db_cursor
from shared.job_counter import job_snapshot

ACTIVE_AIRFLOW_STATES = (
    "deferred",
    "queued",
    "restarting",
    "running",
    "scheduled",
    "up_for_reschedule",
    "up_for_retry",
)
HTTP_TIMEOUT_SECONDS = 3
CONTAINER_HEALTH_URL = os.environ.get(
    "CONTAINER_HEALTH_URL", "http://container-health:9110"
)

SERVICE_EVIDENCE = {
    "archiver_jobs": ("ARCHIVER_URL", "http://archiver:8001", None),
    "pack_worker_jobs": ("PACK_WORKER_URL", "http://pack-worker:8001", None),
    "processing_jobs": ("PROCESSING_URL", "http://processing:8070", None),
    "dbt_runner_jobs": ("DBT_RUNNER_URL", "http://dbt_runner:8080", None),
    "scraper_detail_jobs": ("SCRAPER_URL", "http://scraper:8000", "detail_fetch"),
    "scraper_listing_jobs": ("SCRAPER_URL", "http://scraper:8000", "listing_fetch"),
}


def _iso(value: Any) -> Any:
    return value.isoformat() if isinstance(value, datetime) else value


def _known(source: str, count: int, oldest: Any = None) -> dict[str, Any]:
    return {
        "source": source,
        "status": "known",
        "count": int(count),
        "oldest_started_at": _iso(oldest),
    }


def _unknown(source: str, reason: str) -> dict[str, Any]:
    return {
        "source": source,
        "status": "unknown",
        "count": None,
        "oldest_started_at": None,
        "reason": reason,
    }


def _database_count(source: str, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any]:
    try:
        with db_cursor(error_context=f"Coordination-Drain-{source}") as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
        return _known(source, row[0], row[1])
    except Exception:
        return _unknown(source, "database evidence unavailable")


def _processing_artifacts() -> dict[str, Any]:
    return _database_count("processing_artifacts", SELECT_PROCESSING_ARTIFACTS_BACKLOG)


# Every statement these read is schema-qualified deliberately. The ops role's
# search_path is `ops, staging, public`, so `detail_scrape_claims` (schema
# `ops`) resolved only by luck of naming and the two Airflow tables did not
# resolve at all -- `_database_count` turns that into `unknown`, and unknown
# fails closed, so the drain hung instead of reporting an error.
#
# The three functions below build *parameters* only. Until Plan 162 Stage 9
# two of them built SQL as well, with f-strings sized to their arguments, and
# claimed the contract's "structurally generated statements" exemption. They
# never qualified for it: only the number of values varied, never an
# identifier. Passing the values as arrays makes the text static, so the
# statements live in ops/sql/ and Layer 2 executes the files rather than
# importing a builder to find out what it would have produced.
RUNNING_DETAIL_CLAIMS_SQL = SELECT_RUNNING_DETAIL_CLAIMS


def _running_detail_claims() -> dict[str, Any]:
    return _database_count("running_detail_claims", RUNNING_DETAIL_CLAIMS_SQL)


def task_instance_params(scope: frozenset[str]) -> tuple[Any, ...]:
    """Parameters for SELECT_AIRFLOW_TASK_INSTANCES: dag_ids, task_ids, states.

    The (dag_id, task_id) pairs are passed as two parallel arrays and zipped
    back into rows by multi-argument ``unnest``. An empty scope is not a
    special case: it yields empty arrays, the join matches nothing, and the
    count is zero -- which is the answer the old ``None`` return was
    manufacturing in Python.
    """
    task_pairs = sorted(
        (dag_id, task_id)
        for dag_id, surfaces in ADMISSION_SURFACES.items()
        if surfaces & scope
        for task_id in DRAIN_TASKS[dag_id]
    )
    return (
        [dag_id for dag_id, _ in task_pairs],
        [task_id for _, task_id in task_pairs],
        list(ACTIVE_AIRFLOW_STATES),
    )


def task_instance_query(scope: frozenset[str]) -> tuple[str, tuple[Any, ...]]:
    """The active-task-instance count, as (sql, params)."""
    return (SELECT_AIRFLOW_TASK_INSTANCES, task_instance_params(scope))


def _airflow_task_instances(scope: frozenset[str]) -> dict[str, Any]:
    return _database_count("airflow_task_instances", *task_instance_query(scope))


def gate_observation_params(scope: frozenset[str], generation: int) -> tuple[Any, ...]:
    """Parameters for SELECT_AIRFLOW_GATE_OBSERVATIONS: dag_ids, generation."""
    dag_ids = sorted(
        dag_id for dag_id, surfaces in ADMISSION_SURFACES.items() if surfaces & scope
    )
    return (dag_ids, generation)


def gate_observation_query(
    scope: frozenset[str], generation: int
) -> tuple[str, tuple[Any, ...]]:
    """Active affected DAG runs that have not observed this drain, as (sql, params)."""
    return (SELECT_AIRFLOW_GATE_OBSERVATIONS, gate_observation_params(scope, generation))


def _airflow_gate_observations(
    scope: frozenset[str], generation: int | None
) -> dict[str, Any]:
    """Count active affected DAG runs that have not observed this drain."""
    if not isinstance(generation, int) or generation < 1:
        return _unknown("airflow_gate_observations", "coordination generation unavailable")
    return _database_count(
        "airflow_gate_observations", *gate_observation_query(scope, generation)
    )


def _service_jobs(source: str) -> dict[str, Any]:
    env_name, default_url, surface = SERVICE_EVIDENCE[source]
    try:
        response = requests.get(
            f"{os.environ.get(env_name, default_url).rstrip('/')}/ready",
            timeout=HTTP_TIMEOUT_SECONDS,
        )
        payload = response.json()
        if isinstance(payload, dict) and isinstance(payload.get("detail"), dict):
            payload = payload["detail"]
        if surface is None:
            count = payload["active_jobs"]
            oldest = payload.get("oldest_started_at")
        else:
            count = payload["active_by_surface"][surface]
            oldest = payload["oldest_by_surface"][surface]
        if not isinstance(count, int) or count < 0:
            raise ValueError("invalid active count")
        return _known(source, count, oldest)
    except (requests.RequestException, KeyError, TypeError, ValueError):
        return _unknown(source, "service evidence unavailable or malformed")


def _ops_jobs() -> dict[str, Any]:
    evidence = job_snapshot()
    return _known("ops_jobs", evidence["active_jobs"], evidence["oldest_started_at"])


def _container_processes(scope: frozenset[str]) -> dict[str, Any]:
    """Count live Compose one-offs whose declared surfaces intersect scope."""
    try:
        response = requests.get(
            f"{CONTAINER_HEALTH_URL.rstrip('/')}/oneoff-processes",
            timeout=HTTP_TIMEOUT_SECONDS,
        )
        payload = response.json()
        processes = payload["processes"]
        if payload.get("known") is not True or not isinstance(processes, list):
            raise ValueError("invalid one-off evidence")
        applicable = []
        for process in processes:
            service = process["service"]
            contract = SERVICE_CONTRACTS.get(service)
            if contract is None:
                raise ValueError("unknown one-off service")
            if contract.surfaces & scope:
                applicable.append(process)
        oldest = min(
            (process.get("started_at") for process in applicable if process.get("started_at")),
            default=None,
        )
        return _known("container_processes", len(applicable), oldest)
    except (requests.RequestException, KeyError, TypeError, ValueError):
        return _unknown("container_processes", "container evidence unavailable or malformed")


def _read_source(
    source: str, scope: frozenset[str], generation: int | None = None
) -> dict[str, Any]:
    readers: dict[str, Callable[[], dict[str, Any]]] = {
        "processing_artifacts": _processing_artifacts,
        "running_detail_claims": _running_detail_claims,
        "ops_jobs": _ops_jobs,
    }
    if source == "airflow_task_instances":
        return _airflow_task_instances(scope)
    if source == "airflow_gate_observations":
        return _airflow_gate_observations(scope, generation)
    if source == "container_processes":
        return _container_processes(scope)
    if source in SERVICE_EVIDENCE:
        return _service_jobs(source)
    if source in readers:
        return readers[source]()
    return _unknown(source, "evidence adapter not implemented")


def collect_drain_status(state: dict[str, Any]) -> dict[str, Any]:
    """Read every applicable source without mutating coordination state."""
    scope = frozenset(state.get("scope") or ())
    required = required_drain_sources(scope)
    evidence = []
    for source in sorted(DRAIN_SOURCES):
        if source in required:
            evidence.append(_read_source(source, scope, state.get("generation")))
        else:
            evidence.append(
                {
                    "source": source,
                    "status": "not_applicable",
                    "count": None,
                    "oldest_started_at": None,
                }
            )

    blockers = [
        item["source"]
        for item in evidence
        if item["status"] == "unknown"
        or (item["status"] == "known" and item["count"] > 0)
    ]
    drained = state.get("phase") == "draining" and not blockers
    return {
        "phase": state.get("phase"),
        "scope": sorted(scope),
        "drained": drained,
        "blockers": blockers,
        "sources": evidence,
    }
