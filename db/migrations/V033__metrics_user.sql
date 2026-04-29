-- Plan 86: create metrics_user for postgres-exporter (Prometheus scraping).
-- pg_monitor grants read access to pg_stat_* views without superuser.
-- CREATE is skipped if the user already exists (created manually on server).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'metrics_user') THEN
        CREATE USER metrics_user WITH PASSWORD '${metricsPassword}' CONNECTION LIMIT 3;
    ELSE
        ALTER USER metrics_user WITH PASSWORD '${metricsPassword}' CONNECTION LIMIT 3;
    END IF;
END $$;
GRANT pg_monitor TO metrics_user;
