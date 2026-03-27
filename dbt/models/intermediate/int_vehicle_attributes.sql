{{
  config(
    materialized = 'incremental',
    unique_key = 'vin',
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns'
  )
}}

-- Upstream source of truth for VIN identity.
-- One row per VIN: best make/model/trim/year/dealer resolved with detail > SRP priority.
-- Includes first_seen_at, last_seen_at, and is_tracked flag.

with
{% if is_incremental() %}
changed_vins as (
    select distinct vin17 as vin
    from {{ ref('stg_srp_observations') }}
    where artifact_id > (select coalesce(max(attributes_artifact_id), 0) from {{ this }})
      and vin17 is not null
    union
    select distinct vin17 as vin
    from {{ ref('stg_detail_observations') }}
    where artifact_id > (select coalesce(max(attributes_artifact_id), 0) from {{ this }})
      and vin17 is not null
      and make is not null
),
{% endif %}

srp_attrs as (
    select
        s.vin17                as vin,
        s.make,
        s.model,
        s.vehicle_trim,
        s.model_year,
        s.msrp,
        s.fuel_type,
        s.body_style,
        s.stock_type,
        s.financing_type,
        s.seller_zip,
        s.seller_customer_id,
        s.canonical_detail_url,
        ra.search_key,
        ra.search_scope,
        s.fetched_at,
        s.artifact_id,
        'srp'::text            as attributes_source,
        1                      as source_priority  -- lower = preferred
    from {{ ref('stg_srp_observations') }} s
    inner join {{ ref('stg_raw_artifacts') }} ra
        on ra.artifact_id = s.artifact_id
    where s.vin17 is not null
    {% if is_incremental() %}
      and s.vin17 in (select vin from changed_vins)
    {% endif %}
),

detail_attrs as (
    select
        d.vin17                as vin,
        d.make,
        d.model,
        d.vehicle_trim,
        d.model_year,
        d.msrp,
        d.fuel_type,
        d.body_style,
        d.stock_type,
        null::text             as financing_type,
        d.dealer_zip           as seller_zip,
        d.customer_id          as seller_customer_id,
        d.canonical_detail_url,
        null::text             as search_key,
        null::text             as search_scope,
        d.fetched_at,
        d.artifact_id,
        'detail'::text         as attributes_source,
        0                      as source_priority  -- detail preferred over SRP
    from {{ ref('stg_detail_observations') }} d
    where d.vin17 is not null
      and d.make is not null
    {% if is_incremental() %}
      and d.vin17 in (select vin from changed_vins)
    {% endif %}
),

combined as (
    select * from srp_attrs
    union all
    select * from detail_attrs
),

-- Best attribute row per VIN: detail over SRP, then freshest, then highest artifact_id
ranked as (
    select
        *,
        row_number() over (
            partition by vin
            order by source_priority asc, fetched_at desc, artifact_id desc
        ) as rn
    from combined
),

-- First/last seen across both sources (staging tables hold all history for changed VINs)
vin_timeline as (
    select vin, min(fetched_at) as first_seen_at, max(fetched_at) as last_seen_at
    from combined
    group by vin
),

best as (
    select * from ranked where rn = 1
)

select
    b.vin,
    b.make,
    b.model,
    b.vehicle_trim,
    b.model_year,
    b.msrp,
    b.fuel_type,
    b.body_style,
    b.stock_type,
    b.financing_type,
    b.seller_zip,
    b.seller_customer_id,
    b.canonical_detail_url,
    b.search_key,
    b.search_scope,
    b.fetched_at          as attributes_observed_at,
    b.artifact_id         as attributes_artifact_id,
    b.attributes_source,
    tl.first_seen_at,
    tl.last_seen_at,
    case when tgt.make is not null then true else false end as is_tracked
from best b
inner join vin_timeline tl on tl.vin = b.vin
left join {{ ref('int_scrape_targets') }} tgt
    on tgt.make = b.make and tgt.model = b.model
