{{
  config(materialized='table')
}}

-- Daily detail scrape batch outcomes.
-- extraction_yield = fraction of detail observations with a valid 17-char VIN.
-- Low yield days indicate parser failures or structural page changes on the source site.
-- Replaces: recent_detail_runs.sql (which referenced the deprecated runs/raw_artifacts tables).

select
    date_trunc('day', fetched_at)::date                                        as obs_date,
    count(*)                                                                   as detail_observations,
    count(distinct artifact_id)                                                as detail_artifacts,
    count(*) filter (where vin17 is not null)                                  as valid_vin_count,
    count(distinct vin17) filter (where vin17 is not null)                     as unique_vins_enriched,
    round(
        count(*) filter (where vin17 is not null) * 100.0
        / nullif(count(*), 0), 1
    )                                                                          as extraction_yield
from {{ ref('stg_observations') }}
where source = 'detail'
  and fetched_at is not null
group by date_trunc('day', fetched_at)::date
order by obs_date desc
