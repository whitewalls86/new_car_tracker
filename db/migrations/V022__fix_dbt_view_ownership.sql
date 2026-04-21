-- Fix view ownership so dbt_user (the prod dbt role) can CREATE OR REPLACE.
-- V018 created these views as 'cartracker', but dbt connects as 'dbt_user'.

ALTER VIEW analytics.stg_blocked_cooldown OWNER TO dbt_user;
ALTER VIEW ops.ops_detail_scrape_queue OWNER TO dbt_user;
