-- After ownership of analytics/ops objects was transferred to dbt_user (V006/V007),
-- ALTER DEFAULT PRIVILEGES set by cartracker in V003/V004 no longer apply to
-- objects created by dbt_user. Re-grant on existing objects and set default
-- privileges so future dbt_user-created objects are automatically accessible.

-- ── viewer: analytics ────────────────────────────────────────────────────────
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO viewer;
ALTER DEFAULT PRIVILEGES FOR ROLE dbt_user IN SCHEMA analytics GRANT SELECT ON TABLES TO viewer;

-- ── viewer: ops ──────────────────────────────────────────────────────────────
GRANT SELECT ON ALL TABLES IN SCHEMA ops TO viewer;
ALTER DEFAULT PRIVILEGES FOR ROLE dbt_user IN SCHEMA ops GRANT SELECT ON TABLES TO viewer;

-- ── scraper_user: ops ────────────────────────────────────────────────────────
GRANT SELECT ON ALL TABLES IN SCHEMA ops TO scraper_user;
ALTER DEFAULT PRIVILEGES FOR ROLE dbt_user IN SCHEMA ops GRANT SELECT ON TABLES TO scraper_user;
