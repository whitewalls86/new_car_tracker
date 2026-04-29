-- V034: Drop legacy observation tables and analytics schema (dbt Postgres era).
-- Safe to run as soon as Tracks 1–3 are deployed — no application code depends
-- on these tables any longer.

-- Legacy observation tables (migrated to MinIO silver in Plan 100)
DROP TABLE IF EXISTS public.srp_observations CASCADE;
DROP TABLE IF EXISTS public.detail_observations CASCADE;
DROP TABLE IF EXISTS public.detail_carousel_hints CASCADE;
DROP TABLE IF EXISTS public.pipeline_errors;

-- analytics schema: views first (dependency order)
DROP VIEW IF EXISTS analytics.int_carousel_price_events_mapped;
DROP VIEW IF EXISTS analytics.int_carousel_price_events_unmapped;
DROP VIEW IF EXISTS analytics.int_price_history_by_vin;
DROP VIEW IF EXISTS analytics.int_dealer_inventory;
DROP VIEW IF EXISTS analytics.int_scrape_targets;
DROP VIEW IF EXISTS analytics.stg_blocked_cooldown;
DROP VIEW IF EXISTS analytics.stg_dealers;
DROP VIEW IF EXISTS analytics.stg_search_configs;

-- analytics schema: tables (dbt Postgres era — now in DuckDB)
DROP TABLE IF EXISTS analytics.stg_srp_observations;
DROP TABLE IF EXISTS analytics.stg_detail_observations;
DROP TABLE IF EXISTS analytics.stg_detail_carousel_hints;
DROP TABLE IF EXISTS analytics.stg_raw_artifacts;
DROP TABLE IF EXISTS analytics.int_carousel_hints_filtered;
DROP TABLE IF EXISTS analytics.int_listing_to_vin;
DROP TABLE IF EXISTS analytics.int_latest_dealer_name_by_vin;
DROP TABLE IF EXISTS analytics.int_latest_price_by_vin;
DROP TABLE IF EXISTS analytics.int_latest_tier1_observation_by_vin;
DROP TABLE IF EXISTS analytics.int_listing_current_state;
DROP TABLE IF EXISTS analytics.int_listing_days_on_market;
DROP TABLE IF EXISTS analytics.int_model_price_benchmarks;
DROP TABLE IF EXISTS analytics.int_price_events;
DROP TABLE IF EXISTS analytics.int_price_percentiles_by_vin;
DROP TABLE IF EXISTS analytics.int_vehicle_attributes;
DROP TABLE IF EXISTS analytics.int_vin_current_state;
DROP TABLE IF EXISTS analytics.mart_deal_scores;
DROP TABLE IF EXISTS analytics.mart_vehicle_snapshot;
DROP TABLE IF EXISTS analytics.scrape_targets;
