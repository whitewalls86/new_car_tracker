{{
  config(materialized='view')
}}

-- 403 blocked cooldown lifecycle events from MinIO.
-- One row per event: either 'blocked' (first block) or 'incremented' (subsequent attempt).
-- num_of_attempts is the cumulative count at the time of the event.

select
    event_id,
    listing_id,
    event_type,
    num_of_attempts,
    event_at
from {{ source('ops_events', 'blocked_cooldown_events') }}
