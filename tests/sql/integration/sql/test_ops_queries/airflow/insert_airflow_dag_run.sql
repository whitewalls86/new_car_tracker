INSERT INTO airflow.dag_run (dag_id, run_id, state, start_date, run_type, run_after) VALUES (%s, %s, 'running', now(), 'manual', now())
