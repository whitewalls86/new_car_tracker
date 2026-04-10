-- Grant dbt_user write access to the ops schema so it can create/replace views
-- (e.g. ops_vehicle_staleness). Ownership of existing objects is also transferred
-- since they were previously created by 'cartracker'.

GRANT USAGE, CREATE ON SCHEMA ops TO dbt_user;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA ops TO dbt_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA ops GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO dbt_user;

DO $$
DECLARE
    obj RECORD;
BEGIN
    FOR obj IN
        SELECT tablename AS name, 'TABLE' AS kind
        FROM pg_tables
        WHERE schemaname = 'ops'
        UNION ALL
        SELECT viewname AS name, 'VIEW' AS kind
        FROM pg_views
        WHERE schemaname = 'ops'
    LOOP
        EXECUTE format('ALTER %s ops.%I OWNER TO dbt_user', obj.kind, obj.name);
    END LOOP;
END $$;
