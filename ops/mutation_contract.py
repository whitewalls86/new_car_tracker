"""Mutation-boundary and drain-evidence registry for Plan 142."""

from dataclasses import dataclass


@dataclass(frozen=True)
class DrainSource:
    surfaces: frozenset[str]
    mechanism: str


@dataclass(frozen=True)
class MutationContract:
    surfaces: frozenset[str]
    execution: str
    drain_sources: frozenset[str] = frozenset()
    no_persistent_work_reason: str = ""


def _source(mechanism: str, *surfaces: str) -> DrainSource:
    return DrainSource(frozenset(surfaces), mechanism)


def _tracked(execution: str, source: str, *surfaces: str) -> MutationContract:
    return MutationContract(frozenset(surfaces), execution, frozenset({source}))


def _short(reason: str, *surfaces: str) -> MutationContract:
    return MutationContract(
        frozenset(surfaces),
        "short_transaction",
        no_persistent_work_reason=reason,
    )


DRAIN_SOURCES = {
    "airflow_gate_observations": _source(
        "active Airflow DAG runs observed blocking on the current coordination generation",
        "detail_fetch",
        "listing_fetch",
        "processing",
        "archive",
        "analytics",
        "airflow_control",
        "database",
    ),
    "airflow_task_instances": _source(
        "Airflow metadata task-instance states plus oldest start",
        "detail_fetch",
        "listing_fetch",
        "processing",
        "archive",
        "analytics",
        "airflow_control",
        "database",
    ),
    "archiver_jobs": _source(
        "shared in-process counter exposed by archiver /ready",
        "archive",
        "analytics",
    ),
    "container_processes": _source(
        "checked-in running-set manifest and live container process state",
        "archive",
        "analytics",
        "airflow_control",
    ),
    "dbt_runner_jobs": _source(
        "shared in-process counter exposed by dbt_runner /ready", "analytics"
    ),
    "ops_jobs": _source(
        "shared in-process counter exposed by ops /coordination/local-drain",
        "detail_fetch",
        "processing",
        "archive",
        "analytics",
    ),
    "pack_worker_jobs": _source(
        "shared in-process counter exposed by the pack-worker /ready endpoint",
        "archive",
    ),
    "processing_artifacts": _source(
        "ops.artifacts_queue status=processing plus processing transition time",
        "processing",
    ),
    "processing_jobs": _source(
        "shared in-process counter exposed by processing /ready", "processing"
    ),
    "running_detail_claims": _source(
        "ops.detail_scrape_claims status=running plus claimed_at", "detail_fetch"
    ),
    "scraper_detail_jobs": _source(
        "scraper async registry plus synchronous detail counter", "detail_fetch"
    ),
    "scraper_listing_jobs": _source(
        "scraper async registry partitioned by listing_fetch", "listing_fetch"
    ),
}


_ATOMIC_DB = (
    "The handler completes inside one bounded database transaction and leaves no "
    "background or externally admitted work."
)
_ATOMIC_STATE = (
    "The handler performs one bounded coordination-state transition and leaves no "
    "background work of its own."
)
_DELEGATED_SHORT = (
    "The UI handler delegates a bounded metadata mutation to the owning service; "
    "it does not start a persistent job."
)


# Keys are source-file-relative route declarations. Exact static coverage is
# enforced in tests: adding, removing, or renaming a mutation route requires a
# reviewed execution shape and evidence decision here.
MUTATION_ROUTES = {
    # Archiver and pack-worker share the app implementation but run in separate
    # processes, so their counters are intentionally distinct evidence sources.
    "archiver/app.py:POST:/cleanup/parquet": _tracked("in_process", "archiver_jobs", "archive"),
    "archiver/app.py:POST:/cleanup/parquet/run": _tracked("in_process", "archiver_jobs", "archive"),
    "archiver/app.py:POST:/cleanup/queue": _tracked("in_process", "archiver_jobs", "archive"),
    "archiver/app.py:POST:/cleanup/queue/run": _tracked("in_process", "archiver_jobs", "archive"),
    "archiver/app.py:POST:/flush/silver/run": _tracked(
        "in_process", "archiver_jobs", "archive", "analytics"
    ),
    "archiver/app.py:POST:/compact/silver/run": _tracked(
        "in_process", "archiver_jobs", "archive", "analytics"
    ),
    "archiver/app.py:POST:/pack/bronze/run": _tracked("in_process", "pack_worker_jobs", "archive"),
    "archiver/app.py:POST:/pack/bronze/prune": _tracked(
        "in_process", "pack_worker_jobs", "archive"
    ),
    "archiver/app.py:POST:/pack/bronze/verify": _tracked(
        "in_process", "pack_worker_jobs", "archive"
    ),
    "archiver/app.py:POST:/flush/staging/run": _tracked(
        "in_process", "archiver_jobs", "archive", "analytics"
    ),
    "archiver/app.py:POST:/disk-usage/run": _tracked("in_process", "pack_worker_jobs", "archive"),
    "archiver/app.py:POST:/snapshots/adaptive-refresh/run": _tracked(
        "in_process", "archiver_jobs", "analytics"
    ),
    # Processing owns durable queue rows and an in-process batch boundary. The
    # aggregate drain later requires both sources, not either/or.
    "processing/routers/artifact.py:POST:/process/artifact/{artifact_id}": MutationContract(
        frozenset({"processing"}),
        "durable_and_in_process",
        frozenset({"processing_artifacts", "processing_jobs"}),
    ),
    "processing/routers/batch.py:POST:/process/batch": MutationContract(
        frozenset({"processing"}),
        "durable_and_in_process",
        frozenset({"processing_artifacts", "processing_jobs"}),
    ),
    # Scraper async jobs and synchronous detail fetches now share surface-aware
    # /ready evidence. Fetch acknowledgement is a bounded in-memory deletion.
    "scraper/app.py:POST:/scrape_results": _tracked(
        "async_registry", "scraper_listing_jobs", "listing_fetch"
    ),
    "scraper/app.py:POST:/scrape_results/jobs/{job_id}/fetched": _short(
        "The handler removes one completed in-memory result and admits no new work.",
        "listing_fetch",
        "detail_fetch",
    ),
    "scraper/app.py:POST:/scrape_detail": _tracked(
        "in_process", "scraper_detail_jobs", "detail_fetch"
    ),
    "scraper/app.py:POST:/scrape_detail/batch": _tracked(
        "async_registry", "scraper_detail_jobs", "detail_fetch"
    ),
    "dbt_runner/app.py:POST:/dbt/docs/generate": _tracked(
        "in_process", "dbt_runner_jobs", "analytics"
    ),
    "dbt_runner/app.py:POST:/dbt/build": _tracked("in_process", "dbt_runner_jobs", "analytics"),
    # Ops admission/claim routes.
    "ops/routers/scrape.py:POST:/rotation/advance": _short(_ATOMIC_DB, "listing_fetch"),
    "ops/routers/scrape.py:POST:/claims/claim-batch": _tracked(
        "durable_claim", "running_detail_claims", "detail_fetch"
    ),
    "ops/routers/scrape.py:POST:/claims/release": _short(_ATOMIC_DB, "detail_fetch"),
    "ops/routers/maintenance.py:POST:/expire-orphan-detail-claims": _tracked(
        "in_process", "ops_jobs", "detail_fetch"
    ),
    "ops/routers/maintenance.py:POST:/reap-stuck-processing": _tracked(
        "in_process", "ops_jobs", "processing"
    ),
    "ops/routers/maintenance.py:POST:/evict-delisted-cooldowns": _tracked(
        "in_process", "ops_jobs", "detail_fetch"
    ),
    "ops/routers/maintenance.py:POST:/reconcile-cooldown-cohorts": _tracked(
        "in_process", "ops_jobs", "detail_fetch", "analytics"
    ),
    "ops/routers/deploy.py:POST:/deploy/start": _short(_ATOMIC_STATE, "database"),
    "ops/routers/deploy.py:POST:/deploy/complete": _short(_ATOMIC_STATE, "database"),
    "ops/routers/coordination.py:POST:/request": _short(_ATOMIC_STATE, "database"),
    "ops/routers/coordination.py:POST:/begin-drain": _short(_ATOMIC_STATE, "database"),
    "ops/routers/coordination.py:POST:/authorize": _short(
        "The handler confirms scoped drain evidence and performs one bounded "
        "coordination-state transition without admitting persistent work.",
        "database",
    ),
    # Admin routes either delegate to a tracked long job or perform bounded
    # metadata/coordination transactions.
    "ops/routers/admin.py:POST:/dbt/trigger": _tracked("delegated", "dbt_runner_jobs", "analytics"),
    "ops/routers/admin.py:POST:/dbt/intents": _short(_DELEGATED_SHORT, "analytics"),
    "ops/routers/admin.py:POST:/dbt/intents/{intent_name}/delete": _short(
        _DELEGATED_SHORT, "analytics"
    ),
    "ops/routers/admin.py:POST:/dbt/docs/generate": _tracked(
        "delegated", "dbt_runner_jobs", "analytics"
    ),
    "ops/routers/admin.py:POST:/deploy/start": _short(_ATOMIC_STATE, "database"),
    "ops/routers/admin.py:POST:/deploy/complete": _short(_ATOMIC_STATE, "database"),
    "ops/routers/admin.py:POST:/searches/": _short(_ATOMIC_DB, "listing_fetch", "detail_fetch"),
    "ops/routers/admin.py:POST:/searches/{search_key}": _short(
        _ATOMIC_DB, "listing_fetch", "detail_fetch"
    ),
    "ops/routers/admin.py:POST:/searches/{search_key}/toggle": _short(
        _ATOMIC_DB, "listing_fetch", "detail_fetch"
    ),
    "ops/routers/admin.py:POST:/searches/{search_key}/delete": _short(
        _ATOMIC_DB, "listing_fetch", "detail_fetch"
    ),
    "ops/routers/users.py:POST:/request-access": _short(_ATOMIC_DB, "database"),
    "ops/routers/users.py:POST:/users/{user_id}/role": _short(_ATOMIC_DB, "database"),
    "ops/routers/users.py:POST:/users/{user_id}/revoke": _short(_ATOMIC_DB, "database"),
    "ops/routers/users.py:POST:/access-requests/{req_id}/approve": _short(_ATOMIC_DB, "database"),
    "ops/routers/users.py:POST:/access-requests/{req_id}/deny": _short(_ATOMIC_DB, "database"),
}


# Non-HTTP work cannot be found by the route inventory, so every known worker
# boundary is named separately. `required_sources` are later aggregate inputs;
# absence or unreadability is unknown, never zero.
NON_HTTP_WORK = {
    "airflow_mutating_tasks": {
        "surfaces": frozenset(
            {
                "detail_fetch",
                "listing_fetch",
                "processing",
                "archive",
                "analytics",
                "airflow_control",
                "database",
            }
        ),
        "required_sources": frozenset(
            {"airflow_gate_observations", "airflow_task_instances"}
        ),
    },
    "snapshot-worker": {
        "surfaces": frozenset({"analytics"}),
        "required_sources": frozenset({"airflow_task_instances", "container_processes"}),
    },
    "pack-worker-cli": {
        "surfaces": frozenset({"archive"}),
        "required_sources": frozenset({"container_processes"}),
    },
    "dbt-tools": {
        "surfaces": frozenset({"analytics"}),
        "required_sources": frozenset({"container_processes"}),
    },
}


def required_drain_sources(scope: set[str] | frozenset[str]) -> frozenset[str]:
    """Return evidence required by mutation boundaries intersecting ``scope``."""
    required: set[str] = set()
    for contract in MUTATION_ROUTES.values():
        if contract.surfaces & scope:
            required.update(contract.drain_sources)
    for contract in NON_HTTP_WORK.values():
        if contract["surfaces"] & scope:
            required.update(contract["required_sources"])
    return frozenset(required)
