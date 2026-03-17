{{
  config(
    materialized = 'incremental',
    unique_key = 'vin',
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns'
  )
}}

with
{% if is_incremental() %}
changed_vins as (
    select distinct vin
    from {{ ref('int_price_events') }}
    where artifact_id > (select coalesce(max(price_artifact_id), 0) from {{ this }})
),
{% endif %}

ranked as (
    select
        vin,
        price,
        observed_at as price_observed_at,
        artifact_id as price_artifact_id,
        listing_id as price_listing_id,
        source as price_source,
        tier as price_tier,
        row_number() over (
            partition by vin
            order by observed_at desc, (price is not null) desc, artifact_id desc
        ) as rn
    from {{ ref('int_price_events') }}
    {% if is_incremental() %}
    where vin in (select vin from changed_vins)
    {% endif %}
)

select
    vin,
    price,
    price_observed_at,
    price_artifact_id,
    price_listing_id,
    price_source,
    price_tier
from ranked
where rn = 1
