-- Plan 71: Airflow metadata schema
-- Creates a dedicated role and schema for Airflow's internal metadata DB.
-- airflow_user owns the schema so Airflow can run its own migrations (CREATE, ALTER, DROP).

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow_user') THEN
        CREATE ROLE airflow_user WITH LOGIN PASSWORD '${airflowPassword}';
    END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS airflow AUTHORIZATION airflow_user;

GRANT CONNECT ON DATABASE cartracker TO airflow_user;
GRANT ALL PRIVILEGES ON SCHEMA airflow TO airflow_user;
ALTER ROLE airflow_user SET search_path = airflow;
