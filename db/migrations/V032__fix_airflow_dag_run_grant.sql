-- V032: Re-apply airflow.dag_run grant with the correct guard.
-- V031 guarded on schema existence (information_schema.schemata) which may have
-- fired before Airflow created the dag_run table. This migration re-applies the
-- grant guarded on the table itself, ensuring viewer access is set correctly.

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
