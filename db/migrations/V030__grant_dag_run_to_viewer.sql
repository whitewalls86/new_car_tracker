-- V030: Grant SELECT on Airflow's dag_run table to the viewer role so the
--       dashboard can display recent scrape DAG run history.
--
-- Airflow uses the same cartracker database (public schema), so dag_run
-- is accessible once the viewer role has read permission.
--
-- Guarded: dag_run is created by airflow-init, which runs after Flyway on a
-- fresh stack. If the table doesn't exist yet, this migration skips silently.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'dag_run'
    ) THEN
        GRANT SELECT ON dag_run TO viewer;
    END IF;
END $$;
