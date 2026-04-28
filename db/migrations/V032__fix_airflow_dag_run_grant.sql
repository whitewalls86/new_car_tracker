-- V032: Fix V031 — guard on table existence, not schema existence.
-- V013 creates the airflow schema, so V031's schema-existence check passes in CI
-- even though airflow-init hasn't run yet and dag_run does not exist.
-- This migration applies the grant correctly and is a no-op if already granted.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'airflow' AND table_name = 'dag_run'
    ) THEN
        EXECUTE 'GRANT USAGE ON SCHEMA airflow TO viewer';
        EXECUTE 'GRANT SELECT ON airflow.dag_run TO viewer';
    END IF;
END $$;
