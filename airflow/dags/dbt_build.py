import logging
import os
from datetime import datetime, timedelta

import requests
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator
from sensors import JsonPostError, http_health_sensor, post_json

from airflow import DAG

DBT_RUNNER_URL = "http://dbt_runner:8080"
_TELEGRAM_API = os.environ.get("TELEGRAM_API", "")
_TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

logger = logging.getLogger(__name__)


def _run_dbt_build(**context):
    conf = context["dag_run"].conf or {}

    payload = {}
    if "select" in conf:
        payload["select"] = conf["select"]
    if "full_refresh" in conf:
        payload["full_refresh"] = conf["full_refresh"]

    try:
        result = post_json(f"{DBT_RUNNER_URL}/dbt/build", payload=payload, timeout=600)
        context["ti"].xcom_push(key="result", value=result)
        return result
    except JsonPostError as e:
        context["ti"].xcom_push(key="result", value=e.result)
        if e.result.get("likely_oom"):
            # Plan 123 Phase 0: retrying an OOM-killed build without changing
            # execution conditions just repeats the failure — fail immediately
            # instead of consuming the bounded retry meant for transient
            # infra errors.
            raise AirflowFailException(
                f"dbt build killed by OOM (rc={e.result.get('returncode')}); not retrying"
            ) from e
        raise


def _notify(**context):
    result = context["ti"].xcom_pull(task_ids="dbt_build", key="result")
    ti = context["ti"]

    if not _TELEGRAM_API or not _TELEGRAM_CHAT_ID:
        logger.warning("TELEGRAM_API/TELEGRAM_CHAT_ID not configured - skipping notification")
        return

    lines = [
        "dbt build FAILED",
        f"Run:     {ti.dag_run.run_id}",
        f"Date:    {ti.execution_date}",
    ]

    if result:
        if result.get("cmd"):
            lines.append(f"Command: {result['cmd']}")
        rc = result.get("returncode")
        if rc is not None:
            lines.append(f"Exit:    {rc}")
        error_body = result.get("stderr") or result.get("stdout") or ""
        if error_body:
            lines += ["", error_body[-800:]]

    try:
        requests.post(
            f"https://api.telegram.org/bot{_TELEGRAM_API}/sendMessage",
            json={"chat_id": _TELEGRAM_CHAT_ID, "text": "\n".join(lines)},
            timeout=10,
        )
    except requests.RequestException:
        logger.warning("Failed to send Telegram notification for dbt build failure")


with DAG(
    dag_id="dbt_build",
    schedule=None,  # manual-only; hourly_analytics_refresh owns the scheduled build
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dbt"],
):
    dbt_runner_up = http_health_sensor("dbt_runner", DBT_RUNNER_URL)

    build = PythonOperator(
        task_id="dbt_build",
        python_callable=_run_dbt_build,
        retries=1,
        retry_delay=timedelta(seconds=30),
    )

    notify = PythonOperator(
        task_id="notify",
        python_callable=_notify,
        trigger_rule="one_failed",
    )

    dbt_runner_up >> build >> notify
