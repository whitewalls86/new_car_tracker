{{
  config(materialized='table')
}}

-- Active make/model combinations from ops.tracked_models joined to enabled search configs.
-- tracked_models is populated by the processing service whenever a make/model appears
-- in an SRP result — the same set used to filter carousel hints at ingest time.
-- No slug normalization needed: values are already lowercased by the processing service.

select distinct
    tm.make,
    tm.model
from {{ source('ops', 'tracked_models') }} tm
inner join {{ ref('stg_search_configs') }} cfg
    on cfg.search_key = tm.search_key
   and cfg.enabled = true
