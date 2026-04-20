from datetime import datetime

import requests
from airflow.operators.python import PythonOperator
from sensors import deploy_intent_sensor, http_health_sensor

from airflow import DAG

OPS_URL = "http://ops:8060"


def _expire_orphan_runs():
    resp = requests.post(f"{OPS_URL}/maintenance/expire-orphan-runs", timeout=60)
    resp.raise_for_status()
    return resp.json()


def _expire_orphan_processing_runs():
    resp = requests.post(f"{OPS_URL}/maintenance/expire-orphan-processing-runs", timeout=60)
    resp.raise_for_status()
    return resp.json()


def _reset_stale_artifact_processing():
    resp = requests.post(f"{OPS_URL}/maintenance/reset-stale-artifact-processing", timeout=60)
    resp.raise_for_status()
    return resp.json()


def _expire_orphan_detail_claims():
    resp = requests.post(f"{OPS_URL}/maintenance/expire-orphan-detail-claims", timeout=60)
    resp.raise_for_status()
    return resp.json()


def _expire_orphan_scrape_jobs():
    resp = requests.post(f"{OPS_URL}/maintenance/expire-orphan-scrape-jobs", timeout=60)
    resp.raise_for_status()
    return resp.json()


with DAG(
    dag_id="orphan_checker",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["maintenance"],
):
    ready = deploy_intent_sensor()
    ops_up = http_health_sensor("ops", OPS_URL)

    expire_runs = PythonOperator(
        task_id="expire_orphan_runs",
        python_callable=_expire_orphan_runs,
    )
    expire_processing_runs = PythonOperator(
        task_id="expire_orphan_processing_runs",
        python_callable=_expire_orphan_processing_runs,
    )
    reset_artifact_processing = PythonOperator(
        task_id="reset_stale_artifact_processing",
        python_callable=_reset_stale_artifact_processing,
    )
    expire_detail_claims = PythonOperator(
        task_id="expire_orphan_detail_claims",
        python_callable=_expire_orphan_detail_claims,
    )
    expire_scrape_jobs = PythonOperator(
        task_id="expire_orphan_scrape_jobs",
        python_callable=_expire_orphan_scrape_jobs,
    )

    ready >> ops_up >> [
        expire_runs,
        expire_processing_runs,
        reset_artifact_processing,
        expire_detail_claims,
        expire_scrape_jobs,
    ]
