with base as (
    select
        vin,
        listing_id,

        tier1_observed_at,
        tier1_artifact_id,
        tier1_source,

        current_listing_url,
        tier1_seller_customer_id,
        listing_state,

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
        end as price_age_hours,

        -- Dealer is considered unenriched if we've never captured their street address
        (d.street is null) as dealer_unenriched

    from base b
    left join {{ source('public', 'dealers') }} d
        on d.customer_id = b.tier1_seller_customer_id
),

flags as (
    select
        c.*,

        -- Full details stale if: 7-day age exceeded, OR dealer has never been enriched via detail scrape
        (c.tier1_age_hours > 168.0 or c.dealer_unenriched) as is_full_details_stale,
        (c.price_observed_at is null) or (c.price_age_hours > 24.0) as is_price_stale,

        case
            when c.dealer_unenriched then 'dealer_unenriched'
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

    listing_state,
    current_listing_url,
    tier1_seller_customer_id,

    is_full_details_stale,
    is_price_stale,
    stale_reason

from flags
where listing_state is distinct from 'unlisted'