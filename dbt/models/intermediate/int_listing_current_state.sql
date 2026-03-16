{{ config(materialized='table') }}

-- Replaces the legacy materialized view `listing_current_state`.
-- Determines the most recent listing_state per listing_id from detail observations.
select distinct on (listing_id)
    listing_id,
    listing_state,
    fetched_at as listing_state_seen_at,
    artifact_id as listing_state_artifact_id
from {{ ref('stg_detail_observations') }}
where listing_id is not null
order by listing_id, fetched_at desc, id desc
