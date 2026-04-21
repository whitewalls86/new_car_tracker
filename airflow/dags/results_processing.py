"""
Results Processing DAG — Plan 71 Phase 4.

Triggers the processing service to claim and process pending artifacts
from ops.artifacts_queue. Runs every 5 minutes.

Flow:
  deploy_intent_sensor → processing_up → process_batch
"""
from datetime import datetime

import requests
from airflow.providers.standard.operators.python import PythonOperator
from sensors import deploy_intent_sensor, http_health_sensor

from airflow import DAG

PROCESSING_URL = "http://processing:8070"
BATCH_SIZE = 1000


def _process_batch():
    """POST /process/batch and return the response summary."""
    resp = requests.post(
        f"{PROCESSING_URL}/process/batch",
        params={"batch_size": BATCH_SIZE},
        timeout=300,
    )
    resp.raise_for_status()
    result = resp.json()

    total = result.get("srp_count", 0) + result.get("detail_count", 0)
    if total == 0 and result.get("retry_count", 0) == 0:
        # Nothing to process — normal idle state
        return result

    return result


with DAG(
    dag_id="results_processing",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["processing", "plan71"],
):
    ready = deploy_intent_sensor()
    processing_up = http_health_sensor("processing", PROCESSING_URL)
    process = PythonOperator(task_id="process_batch", python_callable=_process_batch)

    ready >> processing_up >> process
