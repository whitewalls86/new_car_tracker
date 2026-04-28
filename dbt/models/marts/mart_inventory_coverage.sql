{{
  config(materialized='table')
}}

-- Enrichment coverage by make/model.
-- A VIN is "enriched" if its best observation comes from source='detail'.
-- SRP-only VINs have make/model/price but no detailed attributes (trim, mileage, etc.)
-- Used by the data health dashboard to show which makes/models need more detail scraping.

select
    make,
    model,
    count(*)                                                              as total_vins,
    count(*) filter (where source = 'detail')                            as detail_enriched,
    count(*) filter (where source != 'detail')                           as srp_only,
    round(
        count(*) filter (where source = 'detail') * 100.0 / count(*), 1
    )                                                                     as coverage_pct
from {{ ref('int_latest_observation') }}
where make is not null
  and model is not null
group by make, model
order by total_vins desc
