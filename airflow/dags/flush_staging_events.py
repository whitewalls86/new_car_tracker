from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from sensors import deploy_intent_sensor, http_health_sensor, post_json

from airflow import DAG

ARCHIVER_URL = "http://archiver:8001"


def _run_flush():
    return post_json(f"{ARCHIVER_URL}/flush/staging/run", timeout=300)


with DAG(
    dag_id="flush_staging_events",
    schedule=None,  # manual-only; hourly_analytics_refresh owns scheduled flush + dbt order
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["maintenance"],
):
    ready = deploy_intent_sensor()
    archiver_up = http_health_sensor("archiver", ARCHIVER_URL)
    flush = PythonOperator(task_id="flush_staging_events", python_callable=_run_flush)

    ready >> archiver_up >> flush
