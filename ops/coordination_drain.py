"""Scoped, fail-closed drain evidence aggregation for Plan 142."""

import os
from datetime import datetime
from typing import Any, Callable

import requests

from airflow.dags.coordination_contract import ADMISSION_SURFACES, DRAIN_TASKS
from ops.coordination_contract import SERVICE_CONTRACTS
from ops.mutation_contract import DRAIN_SOURCES, required_drain_sources
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
    # Pending and retry rows are backlog, not admitted work, and never block drain.
    return _database_count(
        "processing_artifacts",
        """SELECT COUNT(*), MIN(COALESCE(
                   (SELECT MAX(event_at)
                      FROM staging.artifacts_queue_events e
                     WHERE e.artifact_id = q.artifact_id
                       AND e.status = 'processing'),
                   q.created_at))
             FROM ops.artifacts_queue q
            WHERE q.status = 'processing'""",
    )


# Every table below is schema-qualified deliberately. The ops role's
# search_path is `ops, staging, public`, so `detail_scrape_claims` (schema
# `ops`) resolved only by luck of naming and the two Airflow tables did not
# resolve at all -- `_database_count` turns that into `unknown`, and unknown
# fails closed, so the drain hung instead of reporting an error. The query
# builders are module-level so tests/integration/sql can execute the real SQL.
RUNNING_DETAIL_CLAIMS_SQL = """SELECT COUNT(*), MIN(claimed_at)
     FROM ops.detail_scrape_claims
    WHERE status = 'running'"""


def _running_detail_claims() -> dict[str, Any]:
    return _database_count("running_detail_claims", RUNNING_DETAIL_CLAIMS_SQL)


def task_instance_query(scope: frozenset[str]) -> tuple[str, tuple[Any, ...]] | None:
    """The active-task-instance count, or None when nothing in scope drains."""
    task_pairs = sorted(
        (dag_id, task_id)
        for dag_id, surfaces in ADMISSION_SURFACES.items()
        if surfaces & scope
        for task_id in DRAIN_TASKS[dag_id]
    )
    if not task_pairs:
        return None

    pair_sql = ", ".join(["(%s, %s)"] * len(task_pairs))
    params = tuple(value for pair in task_pairs for value in pair) + ACTIVE_AIRFLOW_STATES
    return (
        f"""SELECT COUNT(*), MIN(ti.start_date)
               FROM airflow.task_instance ti
               JOIN (VALUES {pair_sql}) AS drained(dag_id, task_id)
                 ON drained.dag_id = ti.dag_id AND drained.task_id = ti.task_id
              WHERE ti.state IN ({", ".join(["%s"] * len(ACTIVE_AIRFLOW_STATES))})""",
        params,
    )


def _airflow_task_instances(scope: frozenset[str]) -> dict[str, Any]:
    query = task_instance_query(scope)
    if query is None:
        return _known("airflow_task_instances", 0)
    return _database_count("airflow_task_instances", *query)


def gate_observation_query(
    scope: frozenset[str], generation: int
) -> tuple[str, tuple[Any, ...]] | None:
    """Active affected DAG runs that have not observed this drain, or None."""
    dag_ids = sorted(
        dag_id for dag_id, surfaces in ADMISSION_SURFACES.items() if surfaces & scope
    )
    if not dag_ids:
        return None
    dag_sql = ", ".join(["(%s)"] * len(dag_ids))
    return (
        f"""SELECT COUNT(*), MIN(dr.start_date)
               FROM airflow.dag_run dr
               JOIN (VALUES {dag_sql}) AS affected(dag_id)
                 ON affected.dag_id = dr.dag_id
              WHERE dr.state IN ('queued', 'running')
                AND NOT EXISTS (
                    SELECT 1
                      FROM public.coordination_gate_observations observed
                     WHERE observed.generation = %s
                       AND observed.dag_id = dr.dag_id
                       AND observed.run_id = dr.run_id
                )""",
        tuple(dag_ids) + (generation,),
    )


def _airflow_gate_observations(
    scope: frozenset[str], generation: int | None
) -> dict[str, Any]:
    """Count active affected DAG runs that have not observed this drain."""
    if not isinstance(generation, int) or generation < 1:
        return _unknown("airflow_gate_observations", "coordination generation unavailable")
    query = gate_observation_query(scope, generation)
    if query is None:
        return _known("airflow_gate_observations", 0)
    return _database_count("airflow_gate_observations", *query)


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
