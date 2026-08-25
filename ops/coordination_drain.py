"""Scoped, fail-closed drain evidence aggregation for Plan 142."""

import os
from datetime import datetime
from typing import Any, Callable

import requests

from airflow.dags.coordination_contract import ADMISSION_SURFACES, DRAIN_TASKS
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


def _running_detail_claims() -> dict[str, Any]:
    return _database_count(
        "running_detail_claims",
        """SELECT COUNT(*), MIN(claimed_at)
             FROM public.detail_scrape_claims
            WHERE status = 'running'""",
    )


def _airflow_task_instances(scope: frozenset[str]) -> dict[str, Any]:
    task_pairs = sorted(
        (dag_id, task_id)
        for dag_id, surfaces in ADMISSION_SURFACES.items()
        if surfaces & scope
        for task_id in DRAIN_TASKS[dag_id]
    )
    if not task_pairs:
        return _known("airflow_task_instances", 0)

    pair_sql = ", ".join(["(%s, %s)"] * len(task_pairs))
    params = tuple(value for pair in task_pairs for value in pair) + ACTIVE_AIRFLOW_STATES
    return _database_count(
        "airflow_task_instances",
        f"""SELECT COUNT(*), MIN(ti.start_date)
               FROM task_instance ti
               JOIN (VALUES {pair_sql}) AS drained(dag_id, task_id)
                 ON drained.dag_id = ti.dag_id AND drained.task_id = ti.task_id
              WHERE ti.state IN ({", ".join(["%s"] * len(ACTIVE_AIRFLOW_STATES))})""",
        params,
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


def _read_source(source: str, scope: frozenset[str]) -> dict[str, Any]:
    readers: dict[str, Callable[[], dict[str, Any]]] = {
        "processing_artifacts": _processing_artifacts,
        "running_detail_claims": _running_detail_claims,
        "ops_jobs": _ops_jobs,
    }
    if source == "airflow_task_instances":
        return _airflow_task_instances(scope)
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
            evidence.append(_read_source(source, scope))
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
