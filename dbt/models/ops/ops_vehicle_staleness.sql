with base as (
    select
        vin,
        listing_id,

        tier1_observed_at,
        tier1_artifact_id,
        tier1_source,

        price,
        price_observed_at,
        price_artifact_id,
        price_source,
        price_tier

    from {{ ref('mart_vehicle_snapshot') }}
),

computed as (
    select
        b.*,

        -- Ages (consistent units)
        (now() - b.tier1_observed_at) as tier1_age_interval,
        (now() - b.price_observed_at) as price_age_interval,

        extract(epoch from (now() - b.tier1_observed_at)) / 3600.0 as tier1_age_hours,
        case
            when b.price_observed_at is null then null
            else extract(epoch from (now() - b.price_observed_at)) / 3600.0
        end as price_age_hours

    from base b
),

flags as (
    select
        c.*,

        -- Policy thresholds (hours)
        (c.tier1_age_hours > 168.0) as is_full_details_stale,
        (c.price_observed_at is null) or (c.price_age_hours > 24.0) as is_price_stale,

        case
            when (c.tier1_age_hours > 168.0) then 'full_details'
            when (c.price_observed_at is null) or (c.price_age_hours > 24.0) then 'price_only'
            else 'not_stale'
        end as stale_reason

    from computed c
)

select
    vin,
    listing_id,

    tier1_observed_at,
    tier1_artifact_id,
    tier1_source,

    price,
    price_observed_at,
    price_artifact_id,
    price_source,
    price_tier,

    tier1_age_interval,
    price_age_interval,
    tier1_age_hours,
    price_age_hours,

    is_full_details_stale,
    is_price_stale,
    stale_reason

from flags