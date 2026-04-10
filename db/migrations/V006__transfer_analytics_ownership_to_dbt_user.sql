-- Transfer ownership of all analytics schema objects to dbt_user.
-- Required because tables were previously created by 'cartracker'; dbt_user
-- needs to be owner to ALTER/replace them during incremental model runs.

DO $$
DECLARE
    obj RECORD;
BEGIN
    FOR obj IN
        SELECT tablename AS name, 'TABLE' AS kind
        FROM pg_tables
        WHERE schemaname = 'analytics'
        UNION ALL
        SELECT viewname AS name, 'VIEW' AS kind
        FROM pg_views
        WHERE schemaname = 'analytics'
    LOOP
        EXECUTE format('ALTER %s analytics.%I OWNER TO dbt_user', obj.kind, obj.name);
    END LOOP;
END $$;

ALTER SCHEMA analytics OWNER TO dbt_user;
