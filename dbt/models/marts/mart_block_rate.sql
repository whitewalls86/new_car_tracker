{{
  config(materialized='table')
}}

-- Hourly 403 blocking events.
-- new_blocks       = first-time blocks (event_type = 'blocked').
-- block_increments = repeated attempts on already-blocked listings (event_type = 'incremented').
-- Join to mart_scrape_volume on hour to compute block rate against total observations.
-- One row per hour.

select
    date_trunc('hour', event_at)::timestamp                         as hour,
    count(*) filter (where event_type = 'blocked')                  as new_blocks,
    count(*) filter (where event_type = 'incremented')              as block_increments,
    count(*)                                                        as total_block_events,
    count(distinct listing_id)                                      as unique_listings_blocked,
    max(num_of_attempts)                                            as max_attempts_seen
from {{ ref('stg_blocked_cooldown_events') }}
where event_at is not null
group by 1
order by 1 desc
