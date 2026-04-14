-- dbt_user needs USAGE on dbt_runs_id_seq to INSERT into dbt_runs.
-- V005 granted INSERT on the table but omitted the sequence, causing
-- "SQL execution failed" errors in dbt_runner's _record_run().

GRANT USAGE ON SEQUENCE public.dbt_runs_id_seq TO dbt_user;
