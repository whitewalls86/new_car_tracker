select
    f.listing_id,
    f.artifact_id,
    f.observed_at,
    f.price,
    'detail_carousel'::text as source,
    2::int as tier
from {{ ref('int_carousel_hints_filtered') }} f
left join {{ ref('int_listing_to_vin') }} m
    on m.listing_id = f.listing_id
left join {{ ref('stg_detail_observations') }} d
    on f.listing_id = d.listing_id
where f.is_valid_target = true
  and m.listing_id is null
  and d.vin is null
