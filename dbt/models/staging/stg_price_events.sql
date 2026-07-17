{{
  config(materialized='ephemeral' if target.type == 'spark' else 'view')
}}

-- Plan 125 Gate B: `ephemeral` on spark, `view` elsewhere -- same reason as
-- stg_observations (a persisted Spark view re-qualifies the parquet.`s3a://...`
-- reference against its own catalog and fails). Both mean "no stored data".
--
-- Price observation events from MinIO.
-- One row per write to ops.price_observations — not deduplicated by change,
-- every observation is recorded. Use this for price history and trend analysis.
--
-- event_type values:
--   'upserted' — price written/updated on the HOT table (srp, detail, carousel)
--   'added'    — first time this listing_id seen with a price (srp)
--   'deleted'  — listing went unlisted; price set to NULL

select
    event_id,
    listing_id,
    upper(vin) as vin,
    price,
    make,
    model,
    artifact_id,
    event_type,
    source,
    event_at
from {{ parquet_source('ops_events', 'price_observation_events') }}
where vin is not null
  and event_type != 'deleted'
  and price is not null
  and price > 0
