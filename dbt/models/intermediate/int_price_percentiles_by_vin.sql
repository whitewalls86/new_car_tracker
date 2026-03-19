{{
  config(
    materialized = 'table'
  )
}}

-- National price percentile per VIN within its make/model/trim group.
-- Materialized as a table so mart_deal_scores doesn't re-scan stg_srp_observations
-- + run a PERCENT_RANK window function inline on every build.
-- Must be a full TABLE (not incremental) — PERCENT_RANK requires all rows.

with ranked as (
    select
        s.vin17 as vin,
        percent_rank() over (
            partition by s.make, s.model, s.vehicle_trim
            order by s.price
        ) as national_price_percentile,
        row_number() over (
            partition by s.vin17
            order by s.fetched_at desc, s.artifact_id desc
        ) as rn
    from {{ ref('stg_srp_observations') }} s
    inner join {{ source('public', 'raw_artifacts') }} ra
        on ra.artifact_id = s.artifact_id
    where s.vin17 is not null
      and s.price is not null
      and s.price > 0
      and ra.search_scope = 'national'
      and s.fetched_at >= now() - interval '{{ var("staleness_window_days") }} days'
)

select vin, national_price_percentile
from ranked
where rn = 1
