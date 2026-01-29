with hints as (
    select
        h.artifact_id,
        h.fetched_at as observed_at,
        h.listing_id,
        h.price
    from {{ ref('stg_detail_carousel_hints') }} h
    where h.price is not null
      and h.price > 0
),

mapped as (
    select
        m.vin,
        h.listing_id,
        h.artifact_id,
        h.observed_at,
        h.price
    from hints h
    join {{ ref('int_listing_to_vin') }} m
      on m.listing_id = h.listing_id
)

select
    vin,
    listing_id,
    artifact_id,
    observed_at,
    price,
    'detail_carousel'::text as source,
    2::int as tier
from mapped
