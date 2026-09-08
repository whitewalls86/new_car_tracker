from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from dag_queries import DELETE_STALE_EMAILS_SQL
from sensors import deploy_intent_sensor

from airflow import DAG

with DAG(
    dag_id="delete_stale_emails",
    schedule="0 */2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["maintenance"],
):
    ready = deploy_intent_sensor("delete_stale_emails")

    cleanup = SQLExecuteQueryOperator(
        task_id="delete_stale_emails",
        conn_id="cartracker_db",
        sql=DELETE_STALE_EMAILS_SQL,
    )

    ready >> cleanup
