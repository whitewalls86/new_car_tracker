select
    m.vin,
    f.listing_id,
    f.artifact_id,
    f.observed_at,
    f.price,
    'detail_carousel'::text as source,
    2::int as tier
from {{ ref('int_carousel_hints_filtered') }} f
join {{ ref('int_listing_to_vin') }} m
    on m.listing_id = f.listing_id
where f.is_valid_target = true
