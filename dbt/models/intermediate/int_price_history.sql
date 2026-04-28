{{
  config(materialized='table')
}}

-- Price history per VIN aggregated from the price observation event stream.
-- Price drop/increase counts are derived by comparing consecutive prices with LAG().
--
-- Replaces: int_price_events + int_price_history_by_vin + int_latest_price_by_vin
--           + int_listing_days_on_market

with ordered as (
    select
        vin,
        price,
        event_at,
        lag(price) over (partition by vin order by event_at) as prev_price
    from {{ ref('stg_price_events') }}
)

select
    vin,
    -- Current price = most recent event
    arg_max(price, event_at)                                                  as current_price,
    max(event_at)                                                             as price_observed_at,
    -- First price = oldest event
    arg_min(price, event_at)                                                  as first_price,
    min(price)                                                                as min_price,
    max(price)                                                                as max_price,
    count(*)                                                                  as total_price_observations,
    count(*) filter (where price < prev_price and prev_price is not null)     as price_drop_count,
    count(*) filter (where price > prev_price and prev_price is not null)     as price_increase_count,
    min(event_at)                                                             as first_seen_at,
    max(event_at)                                                             as last_seen_at,
    datediff('day', min(event_at), now())                                     as days_on_market
from ordered
group by vin
