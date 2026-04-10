-- dbt_runner needs write access to operational control tables in the public schema.
-- V003 only granted SELECT on public; these specific tables require more.

GRANT SELECT, UPDATE ON dbt_lock TO dbt_user;
GRANT SELECT, INSERT ON dbt_runs TO dbt_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON dbt_intents TO dbt_user;
