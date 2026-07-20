{{
  config(materialized='table')
}}

-- 403 cooldown attempt distribution.
-- Shows how many listings are stuck in exponential cooldown and at what attempt depth.
-- attempt_bucket groups listings by current attempt count so the shape of the backlog is visible.
-- Source: ops_events.blocked_cooldown_events (flushed from staging.blocked_cooldown_events to MinIO).

with latest_per_listing as (
    select
        listing_id,
        arg_max(num_of_attempts, event_at) as current_attempts,
        arg_max(event_type, event_at)      as latest_event_type
    from {{ ref('stg_blocked_cooldown_events') }}
    group by listing_id
),

-- A listing whose most recent event is 'cleared' is no longer in cooldown.
-- Without this, resolved listings would accumulate in the backlog forever.
active_cooldowns as (
    select listing_id, current_attempts
    from latest_per_listing
    -- IS DISTINCT FROM keeps rows with a null latest_event_type (null <> 'cleared'
    -- would drop them); ANSI, works on both DuckDB and Spark.
    where latest_event_type is distinct from 'cleared'
),

bucketed as (
    select
        listing_id,
        current_attempts,
        case
            when current_attempts = 1 then '1'
            when current_attempts = 2 then '2'
            when current_attempts between 3 and 4 then '3-4'
            when current_attempts between 5 and 10 then '5-10'
            else '11+'
        end as attempt_bucket,
        case
            when current_attempts = 1 then 1
            when current_attempts = 2 then 2
            when current_attempts between 3 and 4 then 3
            when current_attempts between 5 and 10 then 4
            else 5
        end as bucket_order
    from active_cooldowns
)

select
    attempt_bucket,
    bucket_order,
    count(*)              as listing_count,
    min(current_attempts) as min_attempts,
    max(current_attempts) as max_attempts
from bucketed
group by attempt_bucket, bucket_order
order by bucket_order
