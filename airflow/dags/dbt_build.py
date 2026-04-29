import logging
import os
from datetime import datetime, timedelta

import requests
from airflow.providers.standard.operators.python import PythonOperator
from sensors import http_health_sensor

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

    resp = requests.post(
        f"{DBT_RUNNER_URL}/dbt/build",
        json=payload,
        timeout=600,
    )

    if resp.status_code == 409:
        logger.info("dbt build already running (409) — skipping: %s", resp.text)
        return {"ok": True, "skipped": True}

    resp.raise_for_status()
    result = resp.json()
    context["ti"].xcom_push(key="result", value=result)
    return result


def _notify(**context):
    result = context["ti"].xcom_pull(task_ids="dbt_build", key="result")

    if not _TELEGRAM_API or not _TELEGRAM_CHAT_ID:
        logger.warning("TELEGRAM_API/TELEGRAM_CHAT_ID not configured — skipping notification")
        return

    error_detail = ""
    if result:
        error_detail = result.get("stderr") or result.get("stdout") or ""
    msg = f"dbt build FAILED\n\n{error_detail[-500:]}"

    try:
        requests.post(
            f"https://api.telegram.org/bot{_TELEGRAM_API}/sendMessage",
            json={"chat_id": _TELEGRAM_CHAT_ID, "text": msg},
            timeout=10,
        )
    except requests.RequestException:
        logger.warning("Failed to send Telegram notification for dbt build failure")


with DAG(
    dag_id="dbt_build",
    schedule="0 * * * *",  # hourly; override via dag_run.conf {"select": [...]}
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
