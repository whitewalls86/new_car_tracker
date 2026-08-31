import logging
from datetime import datetime, timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator
from notifications import send_failure_alert
from sensors import JsonPostError, deploy_intent_sensor, http_health_sensor, post_json

from airflow import DAG

ARCHIVER_URL = "http://archiver:8001"
DBT_RUNNER_URL = "http://dbt_runner:8080"
OPS_URL = "http://ops:8060"

logger = logging.getLogger(__name__)


def _post_result(context, url: str, timeout: int, payload=None):
    """POST and preserve the response — success or failure — for _notify.

    Plan 134 Stage 1. Without this, a task that raises JsonPostError leaves no
    XCom behind, so the notification has nothing to quote but the DAG's own
    name. ``JsonPostError.result`` is the parsed 500 body, which under Stage 2
    is the endpoint's summary plus its ``failure_reason``; pushing it here is
    what lets the page say which flush broke and why.

    Same shape as pack_bronze_html._post_result, and deliberately so.
    """
    try:
        result = post_json(url, payload=payload, timeout=timeout)
    except JsonPostError as e:
        context["ti"].xcom_push(key="result", value=e.result)
        raise
    context["ti"].xcom_push(key="result", value=result)
    return result


def _run_flush_silver(**context):
    return _post_result(context, f"{ARCHIVER_URL}/flush/silver/run", 300)


def _run_flush_staging(**context):
    return _post_result(context, f"{ARCHIVER_URL}/flush/staging/run", 300)


def _run_reconcile_cooldowns(**context):
    # Runs after the dbt build so it reads the freshly-rebuilt analytics state.
    # Emits 'cleared' events for listings counted as blocked in analytics but
    # gone from the live table; they flush + drop from the mart next cycle.
    return _post_result(
        context, f"{OPS_URL}/maintenance/reconcile-cooldown-cohorts", 180
    )


DEFAULT_DBT_SELECT = ["tag:hourly_core"]


def _run_dbt_build(**context):
    conf = context["dag_run"].conf or {}

    # Plan 123 Phase 1: default to the hourly_core cadence so this scheduled
    # run no longer rebuilds the complete dbt graph every hour. Pass
    # dag_run.conf={"select": [...]} to override — e.g. {"select": []} to
    # build everything (dbt_runner omits --select when the list is empty;
    # "*" fails its SAFE_TOKEN validation) — or trigger the dbt_build DAG
    # directly for a manual full-graph run.
    payload = {"select": DEFAULT_DBT_SELECT}
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


# The tasks this DAG pages about, in execution order. notifications.py names
# whichever of them actually failed and quotes what it left behind.
_WORK_TASKS = (
    "flush_silver_observations",
    "flush_staging_events",
    "dbt_build",
    "reconcile_cooldown_cohorts",
)


def _notify(**context):
    send_failure_alert(context, "hourly analytics refresh FAILED", task_ids=_WORK_TASKS)


with DAG(
    dag_id="hourly_analytics_refresh",
    schedule="0 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["analytics", "dbt"],
):
    ready = deploy_intent_sensor("hourly_analytics_refresh")
    archiver_up = http_health_sensor("archiver", ARCHIVER_URL)
    dbt_runner_up = http_health_sensor("dbt_runner", DBT_RUNNER_URL)

    flush_silver = PythonOperator(
        task_id="flush_silver_observations",
        python_callable=_run_flush_silver,
        retries=1,
        retry_delay=timedelta(seconds=30),
    )
    flush_staging = PythonOperator(
        task_id="flush_staging_events",
        python_callable=_run_flush_staging,
        retries=1,
        retry_delay=timedelta(seconds=30),
    )
    build = PythonOperator(
        task_id="dbt_build",
        python_callable=_run_dbt_build,
        retries=1,
        retry_delay=timedelta(seconds=30),
    )
    reconcile_cooldowns = PythonOperator(
        task_id="reconcile_cooldown_cohorts",
        python_callable=_run_reconcile_cooldowns,
        retries=1,
        retry_delay=timedelta(seconds=30),
    )
    notify = PythonOperator(
        task_id="notify",
        python_callable=_notify,
        trigger_rule="one_failed",
    )

    ready >> archiver_up >> flush_silver >> flush_staging >> dbt_runner_up >> build
    build >> reconcile_cooldowns
    # Plan 140 Stage 4: the sensors are deliberately absent from this fan-in.
    # A health failure used to send "hourly analytics refresh FAILED" — a
    # Telegram message naming the wrong component, which is the defect Plan 140
    # opens with. ct-container-unhealthy reports the service by name instead.
    # The work tasks notify exactly as before.
    [ready, flush_silver, flush_staging, build, reconcile_cooldowns] >> notify
