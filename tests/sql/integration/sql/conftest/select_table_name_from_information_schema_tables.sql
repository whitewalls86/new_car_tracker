SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'airflow'
              AND table_name IN ('task_instance', 'dag_run', 'alembic_version')
