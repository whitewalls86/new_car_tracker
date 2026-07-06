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
    for task_id in [
        "check_deploy_intent",
        "check_archiver_health",
        "flush_silver_observations",
        "flush_staging_events",
        "check_dbt_runner_health",
        "dbt_build",
    ]:
        assert dag.task_dict[task_id] in dag.task_dict["notify"].upstream_list


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
