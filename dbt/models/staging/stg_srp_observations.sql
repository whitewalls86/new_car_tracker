select
  id,
  artifact_id,
  run_id,
  fetched_at,
  listing_id,
  vin,
  case
      when vin is not null and length(vin) = 17 and upper(vin) ~ '^[A-Z0-9]{17}$' then upper(vin)
      else null
  end as vin17,
  seller_customer_id,
  price,
  msrp,
  mileage,
  year as model_year,
  make,
  model,
  "trim" as vehicle_trim,
  stock_type,
  fuel_type,
  body_style,
  financing_type,
  seller_zip,
  page_number,
  position_on_page,
  trid,
  isa_context,
  canonical_detail_url,
  raw_vehicle_json,
  created_at
from {{ source('public', 'srp_observations') }}
