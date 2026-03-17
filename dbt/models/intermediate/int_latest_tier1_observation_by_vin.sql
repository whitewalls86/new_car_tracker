{{
  config(
    materialized = 'incremental',
    unique_key = 'vin',
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns'
  )
}}

with srp as (
    select
      s.vin17 as vin,
      s.listing_id,
      s.artifact_id,
      s.fetched_at as observed_at,
      null::text as listing_state,
      s.mileage,

      s.canonical_detail_url,
      s.seller_customer_id,

      'srp'::text as source
    from {{ ref('stg_srp_observations') }} s
    where s.vin17 is not null
),

detail as (
    select
        d.vin as vin,
        d.listing_id,
        d.artifact_id,
        d.fetched_at as observed_at,
        d.listing_state,
        d.mileage,

        d.canonical_detail_url,
        null::text as seller_customer_id,

        'detail'::text as source
    from {{ ref('stg_detail_observations') }} d
    where d.vin is not null
),

tier1 as (
    select * from srp
    union all
    select * from detail
),

{% if is_incremental() %}
changed_vins as (
    select distinct vin
    from tier1
    where artifact_id > (select coalesce(max(artifact_id), 0) from {{ this }})
),
{% endif %}

ranked as (
    select
        *,
        row_number() over (
            partition by vin
            order by observed_at desc, artifact_id desc
        ) as rn
    from tier1
    {% if is_incremental() %}
    where vin in (select vin from changed_vins)
    {% endif %}
)

select
    vin,
    listing_id,
    artifact_id,
    observed_at,
    listing_state,
    mileage,

    canonical_detail_url,
    seller_customer_id,

    source
from ranked
where rn = 1
