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
changed_srp_listings as (
    select distinct listing_id
    from {{ ref('stg_srp_observations') }}
    where vin17 is not null
      and artifact_id > (select coalesce(max(vin_artifact_id), 0) from {{ this }})
),
changed_detail_listings as (
    select distinct listing_id
    from {{ ref('stg_detail_observations') }}
    where vin17 is not null
      and artifact_id > (select coalesce(max(vin_artifact_id), 0) from {{ this }})
),
changed_listings as (
    select listing_id from changed_srp_listings
    union
    select listing_id from changed_detail_listings
),
{% endif %}

srp_candidates as (
    select
        listing_id,
        vin17 as vin,
        fetched_at as vin_observed_at,
        artifact_id as vin_artifact_id
    from {{ ref('stg_srp_observations') }}
    where vin17 is not null
    {% if is_incremental() %}
      and listing_id in (select listing_id from changed_listings)
    {% endif %}
),

detail_candidates as (
    select
        listing_id,
        vin17 as vin,
        fetched_at as vin_observed_at,
        artifact_id as vin_artifact_id
    from {{ ref('stg_detail_observations') }}
    where vin17 is not null
      and listing_id is not null
    {% if is_incremental() %}
      and listing_id in (select listing_id from changed_listings)
    {% endif %}
),

all_candidates as (
    select * from srp_candidates
    union all
    select * from detail_candidates
),

ranked as (
    select
        listing_id,
        vin,
        vin_observed_at,
        vin_artifact_id,
        row_number() over (
            partition by listing_id
            order by vin_observed_at desc, vin_artifact_id desc
        ) as rn
    from all_candidates
)

select
    listing_id,
    vin,
    vin_observed_at,
    vin_artifact_id
from ranked
where rn = 1
