import logging
from datetime import datetime
from typing import Any, Dict

from airflow.providers.standard.operators.python import PythonOperator
from sensors import deploy_intent_sensor, http_health_sensor, post_json

from airflow import DAG

ARCHIVER_URL = "http://archiver:8001"

logger = logging.getLogger(__name__)

# Plan 120 Phase 4.5: manual/paused DAG that triggers archiver's CI lake
# snapshot exporter. Non-dry-run exports currently return status
# "not_implemented" until the full exporter (Phase 1) lands, so this DAG is
# expected to fail until then when run without dry_run/audit_sources.
DEFAULT_PARAMS: Dict[str, Any] = {
    "tier": "ci",
    "target_vins": 5000,
    "max_archive_mb": 250,
    "source_window_months": 12,
    "min_selector_coverage": True,
    "dry_run": False,
    "audit_sources": False,
    "run_selectors": True,
}

_PASSTHROUGH_KEYS = ("snapshot_id", "max_rows", "source_window_start", "source_window_end")


def build_snapshot_payload(conf: Dict[str, Any]) -> Dict[str, Any]:
    """Merge dag_run conf overrides onto the default snapshot request payload."""
    payload = dict(DEFAULT_PARAMS)
    for key in DEFAULT_PARAMS:
        if key in conf:
            payload[key] = conf[key]
    for key in _PASSTHROUGH_KEYS:
        if key in conf:
            payload[key] = conf[key]
    return payload


def check_snapshot_result(result: Dict[str, Any], payload: Dict[str, Any]) -> None:
    """Raise if the archiver response indicates a failed/unsupported snapshot export."""
    if result.get("skipped"):
        logger.info("export_ci_lake_snapshot: skipped (archiver job already running)")
        return

    status = result.get("status")
    coverage_failures = result.get("coverage_failures") or []

    logger.info(
        "export_ci_lake_snapshot: snapshot_id=%s status=%s archive_key=%s manifest_key=%s "
        "archive_bytes=%s coverage_failures=%s",
        result.get("snapshot_id"),
        status,
        result.get("archive_key"),
        result.get("manifest_key"),
        result.get("archive_bytes"),
        coverage_failures,
    )
    if result.get("source_audit"):
        logger.info("export_ci_lake_snapshot: source_audit=%s", result["source_audit"])
    if result.get("selector_diagnostics"):
        logger.info(
            "export_ci_lake_snapshot: selector_diagnostics=%s", result["selector_diagnostics"]
        )

    if coverage_failures:
        raise RuntimeError(f"snapshot coverage failures: {coverage_failures}")

    audit_sources = payload.get("audit_sources", False)
    dry_run = payload.get("dry_run", False)

    if audit_sources:
        acceptable = {"audited"}
    elif dry_run:
        acceptable = {"planned"}
    else:
        acceptable = {"created"}

    if status not in acceptable:
        raise RuntimeError(
            f"unexpected snapshot status {status!r} for dry_run={dry_run} "
            f"audit_sources={audit_sources} (expected one of {acceptable})"
        )

    if status == "created" and (not result.get("archive_key") or not result.get("manifest_key")):
        raise RuntimeError("created snapshot missing archive_key/manifest_key")


def _run_export(**context):
    # context["params"] already reflects DAG-level defaults overridden by any
    # dag_run.conf keys that match a declared param; conf is merged in on top
    # so ad-hoc keys not declared as params (e.g. snapshot_id) still pass through.
    params = context.get("params") or {}
    conf = context["dag_run"].conf or {}
    payload = build_snapshot_payload({**params, **conf})
    result = post_json(
        f"{ARCHIVER_URL}/snapshots/adaptive-refresh/run",
        payload=payload,
        timeout=3600,
    )
    check_snapshot_result(result, payload)
    return result


with DAG(
    dag_id="export_ci_lake_snapshot",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["maintenance", "snapshots"],
    params=DEFAULT_PARAMS,
):
    ready = deploy_intent_sensor()
    archiver_up = http_health_sensor("archiver", ARCHIVER_URL)
    export = PythonOperator(task_id="export_ci_lake_snapshot", python_callable=_run_export)

    ready >> archiver_up >> export
