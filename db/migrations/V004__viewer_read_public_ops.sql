-- Grant viewer read access to public and ops schemas.
-- The dashboard's pipeline_health page queries runs, scrape_jobs,
-- raw_artifacts, search_configs, dbt_lock, artifact_processing,
-- pipeline_errors, dbt_runs, detail_scrape_claims (public)
-- and ops_detail_scrape_queue, ops_vehicle_staleness (ops).

GRANT USAGE ON SCHEMA public TO viewer;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO viewer;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO viewer;

GRANT USAGE ON SCHEMA ops TO viewer;
GRANT SELECT ON ALL TABLES IN SCHEMA ops TO viewer;
ALTER DEFAULT PRIVILEGES IN SCHEMA ops GRANT SELECT ON TABLES TO viewer;
