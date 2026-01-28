select
  id,
  artifact_id,
  fetched_at,
  listing_id,
  vin,
  listing_state,
  price,
  mileage,
  msrp,
  stock_type,
  dealer_name,
  dealer_zip
from {{ source('public', 'detail_observations') }}
