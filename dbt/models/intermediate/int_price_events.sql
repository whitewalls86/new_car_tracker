{{
  config(
    materialized = 'incremental',
    unique_key = ['artifact_id', 'vin', 'source'],
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns',
    indexes = [
      {'columns': ['vin', 'observed_at'], 'type': 'btree'},
    ],
    post_hook = [
      "CREATE INDEX IF NOT EXISTS idx_int_price_events_vin_observed ON {{ this }} (vin, observed_at)"
    ]
  )
}}

with srp_price_events as (
    select
        -- SRP sometimes carries non-VIN identifiers; vin17 is the cleaned/optional VIN
        s.vin17 as vin,
        s.listing_id,
        s.artifact_id,
        s.fetched_at as observed_at,
        case when s.price is not null and s.price > 0 then s.price else null end as price,
        'srp'::text as source,
        1::int as tier
    from {{ ref('stg_srp_observations') }} s
    where s.vin17 is not null
),

detail_price_events as (
    select
        d.vin17 as vin,
        d.listing_id,
        d.artifact_id,
        d.fetched_at as observed_at,
        case when d.price is not null and d.price > 0 then d.price else null end as price,
        'detail'::text as source,
        1::int as tier
    from {{ ref('stg_detail_observations') }} d
    where d.vin17 is not null
),

carousel_price_events as (
    select
        c.vin,
        c.listing_id,
        c.artifact_id,
        c.observed_at,
        c.price,
        c.source,
        c.tier
    from {{ ref('int_carousel_price_events_mapped') }} c
),

all_events as (
    select * from srp_price_events
    union all
    select * from detail_price_events
    union all
    select * from carousel_price_events
),

-- Deduplicate: when the same VIN has the same price at the same timestamp from
-- multiple sources, keep one row. Prefer detail > srp > carousel.
-- Skipped in incremental mode — new artifacts don't overlap with existing rows.
{% if is_incremental() %}
new_events as (
    select * from all_events
    where artifact_id > (select coalesce(max(artifact_id), 0) from {{ this }})
),
{% endif %}

deduped as (
{% if is_incremental() %}
    select vin, listing_id, artifact_id, observed_at, price, source, tier
    from new_events
{% else %}
    select distinct on (vin, observed_at, price)
        vin, listing_id, artifact_id, observed_at, price, source, tier
    from all_events
    order by vin, observed_at, price,
        case source when 'detail' then 1 when 'srp' then 2 else 3 end
{% endif %}
)

select * from deduped
