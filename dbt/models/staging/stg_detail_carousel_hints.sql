select
  id,
  artifact_id,
  fetched_at,
  source_listing_id,
  listing_id,
  price,
  mileage,
  body,
  condition,
  year
from {{ source('public', 'detail_carousel_hints') }}
