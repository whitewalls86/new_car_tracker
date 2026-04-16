-- Plan 71: Airflow application DB role
-- Least-privilege role for Airflow DAGs that run direct SQL (maintenance DAGs,
-- orphan checker, etc.). Mirrors scraper_user permissions on public/ops schemas,
-- plus explicit grants on auth tables that were locked down in V009.
--
-- NOTE: expand grants here as new DAGs are added that require direct DB access.
-- DAGs that only call HTTP endpoints (scrape_listings, results_processing, etc.)
-- do not require additional grants.

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow_app_user') THEN
        CREATE ROLE airflow_app_user WITH LOGIN PASSWORD '${airflowAppPassword}';
    END IF;
END $$;

GRANT CONNECT ON DATABASE cartracker TO airflow_app_user;

-- ── public schema: same as scraper_user ──────────────────────────────────────

GRANT USAGE ON SCHEMA public TO airflow_app_user;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO airflow_app_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO airflow_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE ON TABLES TO airflow_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE ON SEQUENCES TO airflow_app_user;

-- ── ops schema: read queue views ─────────────────────────────────────────────

GRANT USAGE ON SCHEMA ops TO airflow_app_user;
GRANT SELECT ON ALL TABLES IN SCHEMA ops TO airflow_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA ops GRANT SELECT ON TABLES TO airflow_app_user;

-- ── auth tables: explicit grants (locked down in V009, not covered by defaults) ──
-- delete_stale_emails DAG nulls notification_email on expired access_requests rows.

GRANT SELECT, UPDATE ON public.access_requests TO airflow_app_user;
GRANT SELECT ON public.authorized_users TO airflow_app_user;
