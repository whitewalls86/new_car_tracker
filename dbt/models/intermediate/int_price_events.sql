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
        d.vin as vin,
        d.listing_id,
        d.artifact_id,
        d.fetched_at as observed_at,
        case when d.price is not null and d.price > 0 then d.price else null end as price,
        'detail'::text as source,
        1::int as tier
    from {{ ref('stg_detail_observations') }} d
    where d.vin is not null
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
)

select * from srp_price_events
union all
select * from detail_price_events
union all
select * from carousel_price_events