{{
  config(materialized='view')
}}

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
    vin,
    price,
    make,
    model,
    artifact_id,
    event_type,
    source,
    event_at
from {{ source('ops_events', 'price_observation_events') }}
where vin is not null
  and event_type != 'deleted'
  and price is not null
  and price > 0
