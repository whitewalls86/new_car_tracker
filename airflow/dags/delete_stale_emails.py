from datetime import datetime
from pathlib import Path

from airflow.providers.postgres.operators.postgres import PostgresOperator
from sensors import deploy_intent_sensor

from airflow import DAG

SQL_DELETE_STALE_EMAILS = (
    Path(__file__).parent.parent / "sql" / "delete_stale_emails.sql"
).read_text()

with DAG(
    dag_id="delete_stale_emails",
    schedule="0 */2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["maintenance"],
):
    ready = deploy_intent_sensor()

    cleanup = PostgresOperator(
        task_id="delete_stale_emails",
        postgres_conn_id="cartracker_db",
        sql=SQL_DELETE_STALE_EMAILS,
    )

    ready >> cleanup
