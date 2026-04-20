from datetime import datetime

import requests
from airflow.operators.python import PythonOperator
from sensors import deploy_intent_sensor, http_health_sensor

from airflow import DAG

ARCHIVER_URL = "http://archiver:8001"


def _run_cleanup():
    resp = requests.post(f"{ARCHIVER_URL}/cleanup/artifacts/run", timeout=600)
    resp.raise_for_status()
    return resp.json()


with DAG(
    dag_id="cleanup_artifacts",
    schedule="0 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["maintenance"],
):
    ready = deploy_intent_sensor()
    archiver_up = http_health_sensor("archiver", ARCHIVER_URL)
    cleanup = PythonOperator(task_id="cleanup_artifacts", python_callable=_run_cleanup)

    ready >> archiver_up >> cleanup
