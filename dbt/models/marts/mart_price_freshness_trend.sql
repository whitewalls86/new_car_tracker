{{
  config(materialized='table')
}}

-- Price freshness distribution by make/model.
-- Shows what fraction of tracked VINs have a recent price observation.
-- Useful for detecting when a make/model is being under-scraped.
-- Replaces: price_freshness.sql (which read from the deprecated ops.ops_vehicle_staleness view).

select
    obs.make,
    obs.model,
    count(*)                                                                         as total_vins,
    count(*) filter (
        where datediff('day', ph.price_observed_at, now()) < 1
    )                                                                                as fresh_lt_1d,
    count(*) filter (
        where datediff('day', ph.price_observed_at, now()) between 1 and 3
    )                                                                                as fresh_1_3d,
    count(*) filter (
        where datediff('day', ph.price_observed_at, now()) between 4 and 7
    )                                                                                as fresh_4_7d,
    count(*) filter (
        where datediff('day', ph.price_observed_at, now()) between 8 and 14
    )                                                                                as fresh_8_14d,
    count(*) filter (
        where datediff('day', ph.price_observed_at, now()) > 14
           or ph.price_observed_at is null
    )                                                                                as stale_gt_14d,
    round(
        count(*) filter (
            where ph.price_observed_at is not null
              and datediff('day', ph.price_observed_at, now()) <= 7
        ) * 100.0 / count(*), 1
    )                                                                                as fresh_lt_7d_pct
from {{ ref('int_latest_observation') }} obs
left join {{ ref('int_price_history') }} ph
    on ph.vin = obs.vin17
where obs.make is not null
  and obs.model is not null
  and obs.vin17 is not null
group by obs.make, obs.model
order by stale_gt_14d desc, total_vins desc
