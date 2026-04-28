{{
  config(materialized='table')
}}

-- National market benchmarks per make/model.
-- Computed from current prices across all VINs we track.
-- Used by mart_deal_scores for deal scoring and national comparison.
--
-- Replaces: int_model_price_benchmarks + int_price_percentiles_by_vin

select
    obs.make,
    obs.model,
    count(*)                                                                             as national_listing_count,
    avg(ph.current_price)::int                                                           as national_avg_price,
    percentile_cont(0.5)  within group (order by ph.current_price)::int                 as national_median_price,
    percentile_cont(0.10) within group (order by ph.current_price)::int                 as national_p10_price,
    percentile_cont(0.25) within group (order by ph.current_price)::int                 as national_p25_price,
    percentile_cont(0.75) within group (order by ph.current_price)::int                 as national_p75_price,
    percentile_cont(0.90) within group (order by ph.current_price)::int                 as national_p90_price,
    avg(case when obs.msrp > 0
             then (obs.msrp - ph.current_price)::numeric / obs.msrp * 100
        end)::numeric(5,2)                                                               as national_avg_discount_pct
from {{ ref('int_latest_observation') }} obs
join {{ ref('int_price_history') }} ph on ph.vin = obs.vin17
where ph.current_price > 0
  and obs.make is not null
  and obs.model is not null
group by obs.make, obs.model
