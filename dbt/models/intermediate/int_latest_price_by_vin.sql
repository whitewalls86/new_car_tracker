with ranked as (
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
            order by observed_at desc, artifact_id desc
        ) as rn
    from {{ ref('int_price_events') }}
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
