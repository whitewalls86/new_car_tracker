-- Plan 65: Scoped Postgres roles
-- Creates least-privilege service roles. Passwords injected via Flyway placeholders.
-- Run order: after V001 (schema exists), before services use scoped connections.

-- ── Roles ────────────────────────────────────────────────────────────────────

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'viewer') THEN
        CREATE ROLE viewer WITH LOGIN PASSWORD '${viewerPassword}';
    END IF;

    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'scraper_user') THEN
        CREATE ROLE scraper_user WITH LOGIN PASSWORD '${scraperPassword}';
    END IF;

    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dbt_user') THEN
        CREATE ROLE dbt_user WITH LOGIN PASSWORD '${dbtPassword}';
    END IF;
END $$;

-- ── viewer: SELECT on analytics only ─────────────────────────────────────────

GRANT CONNECT ON DATABASE cartracker TO viewer;
GRANT USAGE ON SCHEMA analytics TO viewer;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO viewer;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT ON TABLES TO viewer;

-- ── scraper_user: write raw data, read search configs ────────────────────────

GRANT CONNECT ON DATABASE cartracker TO scraper_user;

GRANT USAGE ON SCHEMA public TO scraper_user;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO scraper_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO scraper_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE ON TABLES TO scraper_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE ON SEQUENCES TO scraper_user;

-- Read search configs from ops schema
GRANT USAGE ON SCHEMA ops TO scraper_user;
GRANT SELECT ON ALL TABLES IN SCHEMA ops TO scraper_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA ops GRANT SELECT ON TABLES TO scraper_user;

-- ── dbt_user: read raw, write analytics ──────────────────────────────────────

GRANT CONNECT ON DATABASE cartracker TO dbt_user;

-- Read raw source tables
GRANT USAGE ON SCHEMA public TO dbt_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dbt_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO dbt_user;

-- Full write access to analytics (dbt creates/replaces tables and views)
GRANT USAGE, CREATE ON SCHEMA analytics TO dbt_user;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA analytics TO dbt_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA analytics TO dbt_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO dbt_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT USAGE ON SEQUENCES TO dbt_user;
