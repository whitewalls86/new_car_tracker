{{
  config(materialized='table')
}}

-- Hourly scrape throughput by source type.
-- artifact_count  = distinct artifacts processed in the hour (proxy for scrape batches).
-- observation_count = total parsed rows (one row per listing per artifact).
-- vin_extraction_pct = quality signal for detail scrapes; SRP/carousel is always NULL.
-- One row per (hour, source). Source values: 'srp', 'detail', 'carousel'.

select
    date_trunc('hour', fetched_at)::timestamp               as hour,
    source,
    count(distinct artifact_id)                             as artifact_count,
    count(*)                                                as observation_count,
    count(distinct listing_id)                              as unique_listings,
    count(*) filter (where vin17 is not null)               as valid_vin_count,
    case
        when source = 'detail'
        then round(
            count(*) filter (where vin17 is not null) * 100.0
            / nullif(count(*), 0), 1
        )
    end                                                     as vin_extraction_pct
from {{ ref('stg_observations') }}
where fetched_at is not null
group by 1, 2
order by 1 desc, 2
