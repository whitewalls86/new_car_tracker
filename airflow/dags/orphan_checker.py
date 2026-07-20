from datetime import datetime

import requests
from airflow.providers.standard.operators.python import PythonOperator
from sensors import deploy_intent_sensor, http_health_sensor

from airflow import DAG

OPS_URL = "http://ops:8060"


def _expire_orphan_detail_claims():
    resp = requests.post(f"{OPS_URL}/maintenance/expire-orphan-detail-claims", timeout=60)
    resp.raise_for_status()
    return resp.json()


def _reap_stuck_processing():
    resp = requests.post(f"{OPS_URL}/maintenance/reap-stuck-processing", timeout=120)
    resp.raise_for_status()
    return resp.json()


def _evict_delisted_cooldowns():
    resp = requests.post(f"{OPS_URL}/maintenance/evict-delisted-cooldowns", timeout=60)
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

    expire_detail_claims = PythonOperator(
        task_id="expire_orphan_detail_claims",
        python_callable=_expire_orphan_detail_claims,
    )
    reap_stuck_processing = PythonOperator(
        task_id="reap_stuck_processing",
        python_callable=_reap_stuck_processing,
    )
    evict_delisted_cooldowns = PythonOperator(
        task_id="evict_delisted_cooldowns",
        python_callable=_evict_delisted_cooldowns,
    )

    ready >> ops_up >> [
        expire_detail_claims,
        reap_stuck_processing,
        evict_delisted_cooldowns,
    ]
