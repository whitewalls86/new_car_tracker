"""Monthly Plan 131 lifecycle: pack, prune, then verify cold bronze HTML."""

import logging
import os
from datetime import timedelta
from typing import Any, Dict, Optional

import requests

PACK_WORKER_URL = "http://pack-worker:8001"
_TELEGRAM_API = os.environ.get("TELEGRAM_API", "")
_TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

logger = logging.getLogger(__name__)

DEFAULT_PARAMS: Dict[str, Any] = {
    "artifact_type": "detail_page",
    "apply": True,
    "max_buckets": 1,
    "max_packs": 0,
    "prune": True,
    "prune_max_objects": 0,
    "prune_max_packs": 0,
}

_PACK_KEYS = ("artifact_type", "apply", "max_buckets", "max_packs")
_LONG_JOB_TIMEOUT_SECONDS = 43_200


def build_lifecycle_params(conf: Dict[str, Any]) -> Dict[str, Any]:
    """Merge supported DAG-run overrides onto the steady-state defaults."""
    params = dict(DEFAULT_PARAMS)
    for key in DEFAULT_PARAMS:
        if key in conf:
            params[key] = conf[key]
    return params


def check_pack_result(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Validate a pack response and return the first bucket selected for pruning."""
    if result.get("skipped"):
        logger.info("pack_bronze_html: skipped (pack job already running)")
        return None
    if result.get("stopped_for_deploy"):
        raise RuntimeError("pack_bronze_html stopped for deploy; retry when intent clears")
    if result.get("error"):
        raise RuntimeError(f"pack_bronze_html failed: {result['error']}")

    buckets = result.get("buckets") or []
    if not buckets:
        logger.info("pack_bronze_html: no eligible closed month; prune and verify will skip")
        return None

    bucket = buckets[0]
    if bucket.get("stopped_for_deploy"):
        raise RuntimeError("pack_bronze_html stopped for deploy; retry when intent clears")
    if bucket.get("error"):
        raise RuntimeError(f"pack_bronze_html bucket failed: {bucket['error']}")

    if len(buckets) > 1:
        logger.warning(
            "pack_bronze_html: %d buckets were processed; lifecycle tasks use the first (%s-%s)",
            len(buckets),
            bucket.get("year"),
            bucket.get("month"),
        )
    if result.get("read_failures"):
        logger.warning(
            "pack_bronze_html: %d source object(s) were left unpacked after read failures",
            result["read_failures"],
        )
    if bucket.get("orphan_packs"):
        logger.warning(
            "pack_bronze_html: safe orphan pack(s) reported for %s-%s: %s",
            bucket.get("year"),
            bucket.get("month"),
            bucket["orphan_packs"],
        )
    if bucket.get("stopped_at_max_packs"):
        logger.warning(
            "pack_bronze_html: pack cap stopped %s-%s before the bucket drained; "
            "completed sidecars remain safe to prune",
            bucket.get("year"),
            bucket.get("month"),
        )

    year = bucket.get("year")
    month = bucket.get("month")
    if year is None or month is None:
        raise RuntimeError("pack_bronze_html result bucket is missing year/month")
    return {
        "artifact_type": result.get("artifact_type", DEFAULT_PARAMS["artifact_type"]),
        "year": year,
        "month": month,
    }


def check_prune_result(result: Dict[str, Any]) -> None:
    """Validate prune safety signals while allowing resumable warning conditions."""
    if result.get("skipped"):
        logger.info("delete_packed_source_html: skipped (prune job already running)")
        return
    if result.get("stopped_for_deploy"):
        raise RuntimeError("delete_packed_source_html stopped for deploy; retry when intent clears")
    if result.get("error"):
        raise RuntimeError(f"delete_packed_source_html failed: {result['error']}")
    if result.get("objects_refused"):
        raise RuntimeError(
            "delete_packed_source_html refused "
            f"{result['objects_refused']} object(s) during verification"
        )

    if result.get("orphan_packs"):
        logger.warning(
            "delete_packed_source_html: safe orphan pack(s) were ignored: %s",
            result["orphan_packs"],
        )
    if result.get("capped"):
        logger.warning(
            "delete_packed_source_html: run stopped at a cap; the next run will resume safely"
        )


def check_verify_result(result: Dict[str, Any]) -> None:
    """Keep the verifier's endpoint/CLI failure contract true for direct tests too."""
    if result.get("failed"):
        raise RuntimeError(
            f"verify_pack_read_path failed for {result['failed']} sampled member(s)"
        )
    if not result.get("verified"):
        raise RuntimeError("verify_pack_read_path verified no sampled members")


def _merged_context_params(context: Dict[str, Any]) -> Dict[str, Any]:
    params = context.get("params") or {}
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run is not None else {}
    return build_lifecycle_params({**params, **conf})


def _post_result(context: Dict[str, Any], url: str, payload: Dict[str, Any], timeout: int):
    """POST and preserve an HTTP failure summary for the final notification task."""
    from sensors import JsonPostError, post_json

    try:
        result = post_json(url, payload=payload, timeout=timeout)
    except JsonPostError as exc:
        context["ti"].xcom_push(key="result", value=exc.result)
        raise
    context["ti"].xcom_push(key="result", value=result)
    return result


def _run_pack(**context):
    params = _merged_context_params(context)
    payload = {key: params[key] for key in _PACK_KEYS}
    result = _post_result(
        context,
        f"{PACK_WORKER_URL}/pack/bronze/run",
        payload,
        _LONG_JOB_TIMEOUT_SECONDS,
    )
    check_pack_result(result)
    return result


def _run_prune(**context):
    params = _merged_context_params(context)
    pack_result = context["ti"].xcom_pull(task_ids="pack_bronze_html") or {}
    target = check_pack_result(pack_result)
    if target is None:
        result = {"skipped": True, "reason": "pack did not select a bucket"}
        context["ti"].xcom_push(key="result", value=result)
        return result
    if not params["prune"]:
        result = {**target, "skipped": True, "reason": "prune disabled by DAG params"}
        context["ti"].xcom_push(key="result", value=result)
        return result

    payload = {
        **target,
        "apply": params["apply"],
        "max_objects": params["prune_max_objects"],
        "max_packs": params["prune_max_packs"],
    }
    result = _post_result(
        context,
        f"{PACK_WORKER_URL}/pack/bronze/prune",
        payload,
        _LONG_JOB_TIMEOUT_SECONDS,
    )
    check_prune_result(result)
    return result


def _run_verify(**context):
    params = _merged_context_params(context)
    prune_result = context["ti"].xcom_pull(task_ids="prune_packed_source_html") or {}
    year = prune_result.get("year")
    month = prune_result.get("month")
    if year is None or month is None:
        result = {"skipped": True, "reason": "no packed/pruned bucket to verify"}
        context["ti"].xcom_push(key="result", value=result)
        return result

    payload = {
        "artifact_type": prune_result.get("artifact_type", params["artifact_type"]),
        "year": year,
        "month": month,
    }
    result = _post_result(
        context,
        f"{PACK_WORKER_URL}/pack/bronze/verify",
        payload,
        3_600,
    )
    check_verify_result(result)
    return result


def _notify(**context):
    if not _TELEGRAM_API or not _TELEGRAM_CHAT_ID:
        logger.warning("TELEGRAM_API/TELEGRAM_CHAT_ID not configured - skipping notification")
        return

    ti = context["ti"]
    lines = [
        "bronze pack lifecycle FAILED",
        f"Run:     {ti.dag_run.run_id}",
        f"Date:    {ti.execution_date}",
    ]
    for task_id in ("pack_bronze_html", "prune_packed_source_html", "verify_pack_read_path"):
        result = ti.xcom_pull(task_ids=task_id, key="result")
        if not result:
            continue
        reason = result.get("failure_reason") or result.get("error")
        if not reason and result.get("stopped_for_deploy"):
            reason = "deploy intent remained pending through all retries"
        if not reason and result.get("failed"):
            reason = f"{result['failed']} sampled member(s) failed verification"
        if reason:
            lines.append(f"{task_id}: {reason}")
        failures = result.get("failures") or []
        if failures:
            lines.append(f"{task_id} failures: {str(failures)[:800]}")

    try:
        requests.post(
            f"https://api.telegram.org/bot{_TELEGRAM_API}/sendMessage",
            json={"chat_id": _TELEGRAM_CHAT_ID, "text": "\n".join(lines)},
            timeout=10,
        )
    except requests.RequestException:
        logger.warning("Failed to send Telegram notification for bronze pack lifecycle failure")


try:
    import pendulum
    from airflow.providers.standard.operators.python import PythonOperator
    from sensors import deploy_intent_sensor, http_health_sensor

    from airflow import DAG

    with DAG(
        dag_id="pack_bronze_html",
        schedule="0 6 3 * *",
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        tags=["maintenance"],
        params=DEFAULT_PARAMS,
    ) as dag:
        ready = deploy_intent_sensor()
        pack_worker_up = http_health_sensor("pack_worker", PACK_WORKER_URL)
        pack = PythonOperator(
            task_id="pack_bronze_html",
            python_callable=_run_pack,
            retries=6,
            retry_delay=timedelta(minutes=15),
        )
        prune = PythonOperator(
            task_id="prune_packed_source_html",
            python_callable=_run_prune,
            retries=6,
            retry_delay=timedelta(minutes=15),
        )
        verify = PythonOperator(
            task_id="verify_pack_read_path",
            python_callable=_run_verify,
            retries=1,
            retry_delay=timedelta(minutes=15),
        )
        notify = PythonOperator(
            task_id="notify",
            python_callable=_notify,
            trigger_rule="one_failed",
        )

        ready >> pack_worker_up >> pack >> prune >> verify
        [ready, pack_worker_up, pack, prune, verify] >> notify
except ImportError:
    # Keep the result predicates importable in the ordinary unit-test venv.
    # The Airflow integration suite imports the real DAG and asserts it exists.
    pass
