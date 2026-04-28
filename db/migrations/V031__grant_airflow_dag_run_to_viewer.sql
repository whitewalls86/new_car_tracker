-- V031: Fix V030 — dag_run is in the airflow schema, not public.
-- V030 checked the wrong schema so the grant never fired.
-- Also grants USAGE on the airflow schema (required to resolve schema-qualified references).
-- Guarded the same way in case Airflow hasn't initialised yet.

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
