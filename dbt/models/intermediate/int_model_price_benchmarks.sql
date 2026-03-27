-- National price benchmarks by make/model/trim.
-- Uses active listings only (seen in last 3 days) for current market snapshot.
{{
  config(
    materialized = 'table'
  )
 }}

with active_national as (
    select
        s.vin17 as vin,
        s.make,
        s.model,
        s.vehicle_trim,
        s.price,
        s.msrp,
        row_number() over (
            partition by s.vin17
            order by s.fetched_at desc, s.artifact_id desc
        ) as rn
    from {{ ref('stg_srp_observations') }} s
    inner join {{ ref('stg_raw_artifacts') }} ra
        on ra.artifact_id = s.artifact_id
    where s.vin17 is not null
      and s.price is not null
      and s.price > 0
      and ra.search_scope = 'national'
      and s.fetched_at >= now() - interval '{{ var("staleness_window_days") }} days'
),

-- Dedupe: one row per VIN with latest national observation
deduped as (
    select * from active_national where rn = 1
)

select
    make,
    model,
    vehicle_trim,
    count(*) as national_listing_count,
    avg(price)::int as national_avg_price,
    percentile_cont(0.5) within group (order by price)::int as national_median_price,
    percentile_cont(0.10) within group (order by price)::int as national_p10_price,
    percentile_cont(0.25) within group (order by price)::int as national_p25_price,
    percentile_cont(0.75) within group (order by price)::int as national_p75_price,
    percentile_cont(0.90) within group (order by price)::int as national_p90_price,
    avg(msrp)::int as national_avg_msrp,
    avg(case when msrp > 0 then (msrp - price)::numeric / msrp * 100 end)::numeric(5,2) as national_avg_discount_pct
from deduped
where make is not null and model is not null
group by make, model, vehicle_trim
