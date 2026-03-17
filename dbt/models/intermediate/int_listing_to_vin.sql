{{
  config(
    materialized = 'incremental',
    unique_key = 'listing_id',
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns'
  )
}}

with
{% if is_incremental() %}
changed_listings as (
    select distinct listing_id
    from {{ ref('stg_srp_observations') }}
    where vin17 is not null
      and artifact_id > (select coalesce(max(vin_artifact_id), 0) from {{ this }})
),
{% endif %}

candidates as (
    select
        listing_id,
        vin17 as vin,
        fetched_at as vin_observed_at,
        artifact_id as vin_artifact_id,
        row_number() over (
            partition by listing_id
            order by fetched_at desc, artifact_id desc
        ) as rn
    from {{ ref('stg_srp_observations') }}
    where vin17 is not null
    {% if is_incremental() %}
      and listing_id in (select listing_id from changed_listings)
    {% endif %}
)

select
    listing_id,
    vin,
    vin_observed_at,
    vin_artifact_id
from candidates
where rn = 1
