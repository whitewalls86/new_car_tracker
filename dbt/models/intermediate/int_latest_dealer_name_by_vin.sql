{{
  config(
    materialized = 'incremental',
    unique_key = 'vin',
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns'
  )
}}

-- Most recent dealer_name per VIN from detail observations.
-- Interim model until Plan 25.2 bridges UUID<->numeric dealer ID and
-- dealer_name can be joined from the dealers table directly.

{% if is_incremental() %}
with changed_vins as (
    select distinct vin
    from {{ source('public', 'detail_observations') }}
    where artifact_id > (select coalesce(max(artifact_id), 0) from {{ this }})
      and vin is not null
      and dealer_name is not null
),
{% else %}
with
{% endif %}

latest as (
    select distinct on (vin)
        vin,
        dealer_name,
        artifact_id
    from {{ source('public', 'detail_observations') }}
    where vin is not null
      and dealer_name is not null
    {% if is_incremental() %}
      and vin in (select vin from changed_vins)
    {% endif %}
    order by vin, fetched_at desc
)

select vin, dealer_name, artifact_id
from latest
