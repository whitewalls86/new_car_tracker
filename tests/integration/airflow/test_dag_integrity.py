"""
DAG integrity tests.

Verifies that every DAG file in airflow/dags/ can be imported without error
and produces the expected DAG objects. These tests catch broken imports,
syntax errors, and missing tasks before they reach production.

Also validates that service URLs in DAGs match the ports defined in
docker-compose.yml — catches port mismatches before they hit production.

Must be run with PYTHONPATH including airflow/dags/ so that intra-DAG imports
(e.g. `from sensors import ...`) resolve correctly.
"""
import importlib.util
import inspect
import re
import sys
from datetime import timedelta
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parents[3]
DAGS_DIR = REPO_ROOT / "airflow" / "dags"

# Map dag filename -> expected dag_id and expected task_ids
DAG_SPECS = {
    "cleanup_artifacts.py": {
        "dag_id": "cleanup_artifacts",
        "tasks": {"check_deploy_intent", "check_archiver_health", "cleanup_parquet"},
    },
    "cleanup_queue.py": {
        "dag_id": "cleanup_queue",
        "tasks": {"check_deploy_intent", "check_archiver_health", "cleanup_queue"},
    },
    "cleanup_parquet.py": {
        "dag_id": "cleanup_parquet",
        "tasks": {"check_deploy_intent", "check_archiver_health", "cleanup_parquet"},
    },
    "dbt_build.py": {
        "dag_id": "dbt_build",
        "tasks": {"check_dbt_runner_health", "dbt_build", "notify"},
    },
    "flush_silver_observations.py": {
        "dag_id": "flush_silver_observations",
        "tasks": {"check_deploy_intent", "check_archiver_health", "flush_silver_observations"},
    },
    "flush_staging_events.py": {
        "dag_id": "flush_staging_events",
        "tasks": {"check_deploy_intent", "check_archiver_health", "flush_staging_events"},
    },
    "hourly_analytics_refresh.py": {
        "dag_id": "hourly_analytics_refresh",
        "tasks": {
            "check_deploy_intent",
            "check_archiver_health",
            "check_dbt_runner_health",
            "flush_silver_observations",
            "flush_staging_events",
            "dbt_build",
            "reconcile_cooldown_cohorts",
            "notify",
        },
    },
    "delete_stale_emails.py": {
        "dag_id": "delete_stale_emails",
        "tasks": {"check_deploy_intent", "delete_stale_emails"},
    },
    "orphan_checker.py": {
        "dag_id": "orphan_checker",
        "tasks": {
            "check_deploy_intent",
            "check_ops_health",
            "expire_orphan_detail_claims",
            "reap_stuck_processing",
            "evict_delisted_cooldowns",
        },
    },
    "results_processing.py": {
        "dag_id": "results_processing",
        "tasks": {
            "check_deploy_intent",
            "check_processing_health",
            "process_batch",
        },
    },
    "scrape_listings.py": {
        "dag_id": "scrape_listings",
        "tasks": {
            "check_deploy_intent",
            "check_scraper_health",
            "advance_rotation",
            "run_scrapes",
        },
    },
    "scrape_detail_pages.py": {
        "dag_id": "scrape_detail_pages",
        "tasks": {
            "check_deploy_intent",
            "check_scraper_health",
            "claim_batch",
            "scrape_detail",
            "release_claims",
        },
    },
    "compact_silver.py": {
        "dag_id": "compact_silver",
        "tasks": {"check_deploy_intent", "check_archiver_health", "compact_silver"},
    },
    "export_ci_lake_snapshot.py": {
        "dag_id": "export_ci_lake_snapshot",
        "tasks": {"check_deploy_intent", "check_archiver_health", "export_ci_lake_snapshot"},
    },
    "pack_bronze_html.py": {
        "dag_id": "pack_bronze_html",
        "tasks": {
            "check_deploy_intent",
            "check_pack_worker_health",
            "pack_bronze_html",
            "prune_packed_source_html",
            "verify_pack_read_path",
            "notify",
        },
    },
}


def _load_dag_module(filename: str):
    """Import a DAG file as a module, with airflow/dags/ on sys.path."""
    dags_dir = str(DAGS_DIR)
    added = dags_dir not in sys.path
    if added:
        sys.path.insert(0, dags_dir)
    try:
        spec = importlib.util.spec_from_file_location(
            filename.removesuffix(".py"), DAGS_DIR / filename
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    finally:
        if added:
            sys.path.remove(dags_dir)


def _make_dagbag():
    """Build a DagBag across Airflow versions without loading example DAGs."""
    try:
        from airflow.dag_processing.dagbag import DagBag
    except ImportError:
        from airflow.models.dagbag import DagBag

    kwargs = {"dag_folder": str(DAGS_DIR)}
    if "include_examples" in inspect.signature(DagBag).parameters:
        kwargs["include_examples"] = False
    return DagBag(**kwargs)


@pytest.mark.integration
@pytest.mark.parametrize("filename", DAG_SPECS.keys())
def test_dag_imports_without_error(filename):
    """Each DAG file must import cleanly."""
    _load_dag_module(filename)  # raises on any ImportError / syntax error


@pytest.mark.integration
@pytest.mark.parametrize("filename,spec", DAG_SPECS.items())
def test_dag_id_and_tasks(filename, spec):
    """Each DAG must expose the expected dag_id and task set."""
    dagbag = _make_dagbag()

    assert dagbag.import_errors == {}, (
        f"Import errors found: {dagbag.import_errors}"
    )

    dag_id = spec["dag_id"]
    assert dag_id in dagbag.dags, f"DAG '{dag_id}' not found in DagBag"

    dag = dagbag.dags[dag_id]
    actual_tasks = {t.task_id for t in dag.tasks}
    assert actual_tasks == spec["tasks"], (
        f"Task mismatch for '{dag_id}':\n"
        f"  expected: {spec['tasks']}\n"
        f"  actual:   {actual_tasks}"
    )


@pytest.mark.integration
def test_hourly_analytics_refresh_order():
    """Hourly analytics must flush before dbt so dbt reads fresh normalized files."""
    dagbag = _make_dagbag()
    dag = dagbag.dags["hourly_analytics_refresh"]

    assert dag.task_dict["check_deploy_intent"] in (
        dag.task_dict["check_archiver_health"].upstream_list
    )
    assert dag.task_dict["check_archiver_health"] in (
        dag.task_dict["flush_silver_observations"].upstream_list
    )
    assert dag.task_dict["flush_silver_observations"] in (
        dag.task_dict["flush_staging_events"].upstream_list
    )
    assert dag.task_dict["flush_staging_events"] in (
        dag.task_dict["check_dbt_runner_health"].upstream_list
    )
    assert dag.task_dict["check_dbt_runner_health"] in dag.task_dict["dbt_build"].upstream_list
    # The health sensors are deliberately absent from this list -- Plan 140
    # Stage 4. They gate the chain above, but feeding the Telegram task meant
    # an unreachable archiver sent "hourly analytics refresh FAILED", naming
    # the DAG rather than the service that was down.
    for task_id in [
        "check_deploy_intent",
        "flush_silver_observations",
        "flush_staging_events",
        "dbt_build",
    ]:
        assert dag.task_dict[task_id] in dag.task_dict["notify"].upstream_list

    for task_id in ("check_archiver_health", "check_dbt_runner_health"):
        assert dag.task_dict[task_id] not in dag.task_dict["notify"].upstream_list, (
            f"{task_id} feeds the notify task again. A health failure would "
            "send a Telegram message named after the DAG rather than the "
            "service, which is the defect Plan 140 Stage 4 removed."
        )


@pytest.mark.integration
def test_maintenance_pool_reaches_the_real_operators():
    """Plan 142 Stage 0 item 3, Phase A.

    tests/airflow/test_maintenance_pool.py owns the contract and reads the
    source; this checks the attribute actually survives DAG parsing onto the
    task, and that everything else stays on `default_pool` — a task that
    silently landed in `maintenance` would stop running the moment a window
    held it."""
    if str(DAGS_DIR) not in sys.path:
        sys.path.insert(0, str(DAGS_DIR))
    from pools import MAINTENANCE_POOL  # noqa: PLC0415 -- resolved via DAGS_DIR above

    expected = {
        ("results_processing", "process_batch"),
        ("orphan_checker", "expire_orphan_detail_claims"),
        ("orphan_checker", "reap_stuck_processing"),
        ("orphan_checker", "evict_delisted_cooldowns"),
        ("scrape_detail_pages", "claim_batch"),
    }

    dagbag = _make_dagbag()
    assert dagbag.import_errors == {}, f"Import errors found: {dagbag.import_errors}"

    pooled = {
        (dag_id, task.task_id)
        for dag_id, dag in dagbag.dags.items()
        for task in dag.tasks
        if task.pool == MAINTENANCE_POOL
    }
    assert pooled == expected


@pytest.mark.integration
def test_health_sensors_skip_rather_than_fail_on_the_real_operators():
    """Plan 140 Stage 4b, on the parsed task rather than on the source.

    tests/airflow/test_health_sensor_demotion.py owns the contract and reads
    the factory with `ast`; this is what proves the keyword survives onto every
    real sensor across every DAG. Without it a timeout raises
    AirflowSensorTimeout, fails the run, and pages as "DAG {dag_id} failed" --
    named after a downstream consumer rather than the service that is down.

    `check_deploy_intent` is asserted the other way on purpose: a stuck deploy
    intent is Plan 142 Stage 1's condition, and skipping it would let work
    start mid-deploy.
    """
    dagbag = _make_dagbag()
    assert not dagbag.import_errors

    health_sensors = 0
    for dag in dagbag.dags.values():
        for task_id, task in dag.task_dict.items():
            if task_id == "check_deploy_intent":
                assert getattr(task, "soft_fail", False) is False, (
                    f"{dag.dag_id}.{task_id} now skips on timeout; a stuck "
                    "deploy intent must still stop the DAG"
                )
            elif task_id.startswith("check_") and task_id.endswith("_health"):
                health_sensors += 1
                assert task.soft_fail is True, (
                    f"{dag.dag_id}.{task_id} fails instead of skipping on "
                    "timeout, so a down service pages as a DAG failure again"
                )
                assert task.mode == "reschedule", (
                    f"{dag.dag_id}.{task_id} left reschedule mode. Deferrable "
                    "sensors ignore soft_fail on timeout (apache/airflow#61130)"
                )

    assert health_sensors == 16, (
        f"found {health_sensors} health sensors across the DagBag, expected 16. "
        "These gate DAG correctness independently of who reports the outage, "
        "so a dropped one is work starting against an unanswering service."
    )


@pytest.mark.integration
def test_pack_bronze_html_lifecycle_contract():
    """The monthly lifecycle stays UTC, single-run, ordered, and retryable."""
    dagbag = _make_dagbag()
    dag = dagbag.dags["pack_bronze_html"]

    assert dag.schedule == "0 6 3 * *"
    assert str(dag.timezone) == "UTC"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert set(dag.tags) == {"maintenance"}

    ordered_tasks = [
        "check_deploy_intent",
        "check_pack_worker_health",
        "pack_bronze_html",
        "prune_packed_source_html",
        "verify_pack_read_path",
    ]
    for upstream, downstream in zip(ordered_tasks, ordered_tasks[1:]):
        assert dag.task_dict[upstream] in dag.task_dict[downstream].upstream_list

    for task_id in ("pack_bronze_html", "prune_packed_source_html"):
        assert dag.task_dict[task_id].retries == 6
        assert dag.task_dict[task_id].retry_delay == timedelta(minutes=15)


# ---------------------------------------------------------------------------
# Service URL / port validation
# ---------------------------------------------------------------------------

def _parse_compose_ports():
    """
    Parse docker-compose.yml to build a map of service_name → set of
    internal ports (the container-side port from "host:container" mappings,
    plus CMD/ENTRYPOINT ports from Dockerfiles).
    """
    import yaml

    compose_path = REPO_ROOT / "docker-compose.yml"
    with open(compose_path) as f:
        compose = yaml.safe_load(f)

    service_ports = {}
    for name, svc in compose.get("services", {}).items():
        ports = set()
        for p in svc.get("ports", []):
            # "8070:8070" or "9000:9000"
            parts = str(p).split(":")
            if len(parts) == 2:
                ports.add(int(parts[1]))
        # Also check Dockerfile CMD for uvicorn --port
        dockerfile = svc.get("build", {}).get("dockerfile")
        if dockerfile:
            df_path = REPO_ROOT / dockerfile
            if df_path.exists():
                content = df_path.read_text()
                m = re.search(r"--port[=\s]+(\d+)", content)
                if m:
                    ports.add(int(m.group(1)))
        if ports:
            service_ports[name] = ports
    return service_ports


def _extract_dag_service_urls():
    """
    Scan all DAG files for http://<service>:<port> patterns.
    Returns list of (filename, service, port) tuples.
    """
    url_re = re.compile(r'http://(\w+):(\d+)')
    results = []
    for dag_file in DAGS_DIR.glob("*.py"):
        content = dag_file.read_text()
        for m in url_re.finditer(content):
            service = m.group(1)
            port = int(m.group(2))
            results.append((dag_file.name, service, port))
    return results


@pytest.mark.integration
def test_dag_service_urls_match_compose_ports():
    """
    Every http://service:port in a DAG file must reference a port that
    the service actually exposes in docker-compose.yml or its Dockerfile.
    """
    compose_ports = _parse_compose_ports()
    dag_urls = _extract_dag_service_urls()

    mismatches = []
    for filename, service, port in dag_urls:
        known_ports = compose_ports.get(service, set())
        if not known_ports:
            continue  # service not in compose (e.g. external)
        if port not in known_ports:
            mismatches.append(
                f"{filename}: {service}:{port} — "
                f"compose/Dockerfile has {known_ports}"
            )

    assert not mismatches, (
        "DAG service URLs reference ports that don't match "
        "docker-compose.yml:\n  " + "\n  ".join(mismatches)
    )
