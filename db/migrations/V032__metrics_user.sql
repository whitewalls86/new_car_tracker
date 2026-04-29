-- Plan 86: create metrics_user for postgres-exporter (Prometheus scraping).
-- pg_monitor grants read access to pg_stat_* views without superuser.
CREATE USER metrics_user WITH PASSWORD '${metricsPassword}' CONNECTION LIMIT 3;
GRANT pg_monitor TO metrics_user;
