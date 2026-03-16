{{ config(materialized='table') }}

-- Replaces the legacy materialized view `vin_current_state`.
-- Determines the most recent listing_state per VIN from detail observations.
select distinct on (vin)
    vin,
    listing_state,
    fetched_at as state_seen_at,
    artifact_id as state_artifact_id
from {{ ref('stg_detail_observations') }}
where vin is not null and vin <> ''
order by vin, fetched_at desc, id desc
