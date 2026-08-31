import logging
from datetime import datetime, timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator
from notifications import send_failure_alert
from sensors import JsonPostError, deploy_intent_sensor, http_health_sensor, post_json

from airflow import DAG

DBT_RUNNER_URL = "http://dbt_runner:8080"

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
    send_failure_alert(context, "dbt build FAILED", task_ids=("dbt_build",))


with DAG(
    dag_id="dbt_build",
    schedule=None,  # manual-only; hourly_analytics_refresh owns the scheduled build
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dbt"],
):
    ready = deploy_intent_sensor("dbt_build")
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

    ready >> dbt_runner_up >> build >> notify
