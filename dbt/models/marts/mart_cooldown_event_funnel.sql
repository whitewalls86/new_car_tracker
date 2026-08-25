{{
  config(
    materialized='table',
    file_format='iceberg' if target.type == 'spark' else none
  )
}}

-- Hourly flow of detail scrapes into each exponential-cooldown attempt bucket.
-- This is event history, not the current backlog: every blocked/incremented
-- transition counts once, while lifecycle-only cleared events are excluded.

with bucketed as (
    select
        cast(date_trunc('hour', event_at) as timestamp) as event_hour,
        listing_id,
        case
            when num_of_attempts = 1 then '1'
            when num_of_attempts = 2 then '2'
            when num_of_attempts between 3 and 4 then '3-4'
            when num_of_attempts between 5 and 10 then '5-10'
            else '11+'
        end as attempt_bucket,
        case
            when num_of_attempts = 1 then 1
            when num_of_attempts = 2 then 2
            when num_of_attempts between 3 and 4 then 3
            when num_of_attempts between 5 and 10 then 4
            else 5
        end as bucket_order
    from {{ ref('stg_blocked_cooldown_events') }}
    where event_at is not null
      and num_of_attempts >= 1
      and event_type in ('blocked', 'incremented')
)

select
    event_hour,
    attempt_bucket,
    bucket_order,
    count(*) as scrape_count,
    count(distinct listing_id) as unique_listing_count
from bucketed
group by event_hour, attempt_bucket, bucket_order
order by event_hour desc, bucket_order
