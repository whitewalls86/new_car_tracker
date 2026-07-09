"""
Unit tests for the export_ci_lake_snapshot DAG's helper functions (Plan 120
Phase 4.5). Exercises payload building and result validation directly,
without running a real Airflow scheduler or archiver.
"""
import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parents[3]
DAGS_DIR = REPO_ROOT / "airflow" / "dags"


def _load_dag_module():
    dags_dir = str(DAGS_DIR)
    added = dags_dir not in sys.path
    if added:
        sys.path.insert(0, dags_dir)
    try:
        spec = importlib.util.spec_from_file_location(
            "export_ci_lake_snapshot", DAGS_DIR / "export_ci_lake_snapshot.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    finally:
        if added:
            sys.path.remove(dags_dir)


@pytest.fixture(scope="module")
def dag_module():
    return _load_dag_module()


@pytest.mark.integration
def test_build_snapshot_payload_uses_defaults(dag_module):
    payload = dag_module.build_snapshot_payload({})
    assert payload == dag_module.DEFAULT_PARAMS


@pytest.mark.integration
def test_build_snapshot_payload_applies_conf_overrides(dag_module):
    payload = dag_module.build_snapshot_payload(
        {"tier": "dev", "target_vins": 100, "snapshot_id": "adaptive-refresh-pinned"}
    )
    assert payload["tier"] == "dev"
    assert payload["target_vins"] == 100
    assert payload["snapshot_id"] == "adaptive-refresh-pinned"
    # Untouched defaults remain.
    assert payload["max_archive_mb"] == dag_module.DEFAULT_PARAMS["max_archive_mb"]


@pytest.mark.integration
def test_check_snapshot_result_coverage_failed_status_raises(dag_module):
    """require_selector_coverage=True surfaces as status="coverage_failed",
    which is not an acceptable status for a real (non-dry-run) export."""
    payload = dag_module.build_snapshot_payload({})
    result = {
        "snapshot_id": "adaptive-refresh-x",
        "status": "coverage_failed",
        "coverage_failures": ["cooldown_bucket_11_plus: found 0, required 1"],
    }
    with pytest.raises(RuntimeError, match="unexpected snapshot status"):
        dag_module.check_snapshot_result(result, payload)


@pytest.mark.integration
def test_check_snapshot_result_coverage_failures_do_not_block_by_default(dag_module):
    """Coverage shortfalls are non-blocking by default (Plan 120 selector
    policy correction) — they're logged, not raised, as long as status
    itself is an acceptable one."""
    payload = dag_module.build_snapshot_payload({})
    result = {
        "snapshot_id": "adaptive-refresh-x",
        "status": "created",
        "archive_key": "k",
        "manifest_key": "m",
        "coverage_failures": ["relisted_vin: found 1, required 10"],
    }
    dag_module.check_snapshot_result(result, payload)  # no raise


@pytest.mark.integration
def test_check_snapshot_result_created_requires_keys(dag_module):
    payload = dag_module.build_snapshot_payload({})
    result = {
        "snapshot_id": "adaptive-refresh-x",
        "status": "created",
        "archive_key": None,
        "manifest_key": None,
        "coverage_failures": [],
    }
    with pytest.raises(RuntimeError, match="archive_key/manifest_key"):
        dag_module.check_snapshot_result(result, payload)


@pytest.mark.integration
def test_check_snapshot_result_created_passes_with_keys(dag_module):
    payload = dag_module.build_snapshot_payload({})
    result = {
        "snapshot_id": "adaptive-refresh-x",
        "status": "created",
        "archive_key": "ci_snapshots/.../snapshot.tar.zst",
        "manifest_key": "ci_snapshots/.../manifest.json",
        "archive_bytes": 123,
        "coverage_failures": [],
    }
    dag_module.check_snapshot_result(result, payload)  # no raise


@pytest.mark.integration
def test_check_snapshot_result_planned_allowed_only_when_dry_run(dag_module):
    dry_run_payload = dag_module.build_snapshot_payload({"dry_run": True})
    result = {"snapshot_id": "adaptive-refresh-x", "status": "planned", "coverage_failures": []}
    dag_module.check_snapshot_result(result, dry_run_payload)  # no raise

    non_dry_run_payload = dag_module.build_snapshot_payload({"dry_run": False})
    with pytest.raises(RuntimeError, match="unexpected snapshot status"):
        dag_module.check_snapshot_result(result, non_dry_run_payload)


@pytest.mark.integration
def test_check_snapshot_result_audited_allowed_only_when_audit_sources(dag_module):
    audit_payload = dag_module.build_snapshot_payload({"audit_sources": True})
    result = {"snapshot_id": "adaptive-refresh-x", "status": "audited", "coverage_failures": []}
    dag_module.check_snapshot_result(result, audit_payload)  # no raise

    non_audit_payload = dag_module.build_snapshot_payload({"audit_sources": False})
    with pytest.raises(RuntimeError, match="unexpected snapshot status"):
        dag_module.check_snapshot_result(result, non_audit_payload)


@pytest.mark.integration
def test_check_snapshot_result_not_implemented_fails(dag_module):
    payload = dag_module.build_snapshot_payload({})
    result = {
        "snapshot_id": "adaptive-refresh-x",
        "status": "not_implemented",
        "coverage_failures": [],
    }
    with pytest.raises(RuntimeError, match="unexpected snapshot status"):
        dag_module.check_snapshot_result(result, payload)


@pytest.mark.integration
def test_check_snapshot_result_skipped_short_circuits(dag_module):
    payload = dag_module.build_snapshot_payload({})
    result = {"ok": True, "skipped": True}
    dag_module.check_snapshot_result(result, payload)  # no raise
