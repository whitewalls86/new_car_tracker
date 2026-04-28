{{
  config(materialized='table')
}}

-- Price freshness distribution by make/model — active listings only.
-- Shows what fraction of currently-active tracked VINs have a recent price observation.
-- Useful for detecting when a make/model is being under-scraped.
-- Unlisted vehicles are excluded: stale prices on sold/removed listings are expected
-- and would inflate stale_gt_14d, masking real scraping gaps.
-- Replaces: price_freshness.sql (which read from the deprecated ops.ops_vehicle_staleness view).

select
    make,
    model,
    count(*)                                                                         as total_vins,
    count(*) filter (
        where datediff('day', price_observed_at, now()) < 1
    )                                                                                as fresh_lt_1d,
    count(*) filter (
        where datediff('day', price_observed_at, now()) between 1 and 3
    )                                                                                as fresh_1_3d,
    count(*) filter (
        where datediff('day', price_observed_at, now()) between 4 and 7
    )                                                                                as fresh_4_7d,
    count(*) filter (
        where datediff('day', price_observed_at, now()) between 8 and 14
    )                                                                                as fresh_8_14d,
    count(*) filter (
        where price_observed_at is null
           or datediff('day', price_observed_at, now()) > 14
    )                                                                                as stale_gt_14d,
    round(
        count(*) filter (
            where price_observed_at is not null
              and datediff('day', price_observed_at, now()) <= 7
        ) * 100.0 / count(*), 1
    )                                                                                as fresh_lt_7d_pct
from {{ ref('mart_vehicle_snapshot') }}
where listing_state = 'active'
group by make, model
order by stale_gt_14d desc, total_vins desc
