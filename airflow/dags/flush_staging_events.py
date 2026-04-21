from datetime import datetime

import requests
from airflow.providers.standard.operators.python import PythonOperator
from sensors import deploy_intent_sensor, http_health_sensor

from airflow import DAG

ARCHIVER_URL = "http://archiver:8001"


def _run_flush():
    resp = requests.post(f"{ARCHIVER_URL}/flush/staging/run", timeout=300)
    resp.raise_for_status()
    return resp.json()


with DAG(
    dag_id="flush_staging_events",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["maintenance"],
):
    ready = deploy_intent_sensor()
    archiver_up = http_health_sensor("archiver", ARCHIVER_URL)
    flush = PythonOperator(task_id="flush_staging_events", python_callable=_run_flush)

    ready >> archiver_up >> flush
