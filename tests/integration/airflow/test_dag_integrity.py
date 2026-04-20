"""
DAG integrity tests.

Verifies that every DAG file in airflow/dags/ can be imported without error
and produces the expected DAG objects. These tests catch broken imports,
syntax errors, and missing tasks before they reach production.

Must be run with PYTHONPATH including airflow/dags/ so that intra-DAG imports
(e.g. `from sensors import ...`) resolve correctly.
"""
import importlib.util
import sys
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).parents[3] / "airflow" / "dags"

# Map dag filename -> expected dag_id and expected task_ids
DAG_SPECS = {
    "cleanup_artifacts.py": {
        "dag_id": "cleanup_artifacts",
        "tasks": {"check_deploy_intent", "check_archiver_health", "cleanup_artifacts"},
    },
    "cleanup_parquet.py": {
        "dag_id": "cleanup_parquet",
        "tasks": {"check_deploy_intent", "check_archiver_health", "cleanup_parquet"},
    },
    "dbt_build.py": {
        "dag_id": "dbt_build",
        "tasks": {"check_deploy_intent", "check_dbt_runner_health", "dbt_build", "notify"},
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
            "expire_orphan_runs",
            "expire_orphan_processing_runs",
            "reset_stale_artifact_processing",
            "expire_orphan_detail_claims",
            "expire_orphan_scrape_jobs",
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


@pytest.mark.integration
@pytest.mark.parametrize("filename", DAG_SPECS.keys())
def test_dag_imports_without_error(filename):
    """Each DAG file must import cleanly."""
    _load_dag_module(filename)  # raises on any ImportError / syntax error


@pytest.mark.integration
@pytest.mark.parametrize("filename,spec", DAG_SPECS.items())
def test_dag_id_and_tasks(filename, spec):
    """Each DAG must expose the expected dag_id and task set."""
    from airflow.models import DAG
    from airflow.models.dagbag import DagBag

    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)

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
