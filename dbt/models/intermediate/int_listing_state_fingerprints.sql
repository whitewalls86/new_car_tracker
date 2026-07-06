{{
  config(materialized='table')
}}

-- One row per detail artifact with a valid VIN.
-- parsed_fingerprint hashes all business-state fields. Field inclusion is intentional:
--   listing_id:    included — a relisting (same VIN, new listing_id) is a material state
--                  change and must open a new run without special-case logic
--   listing_state: included — active→unavailable transitions are state changes
--   customer_id:   included — dealer identity is business state; same VIN moved to a
--                  different dealer should open a new run
--   seller_id:     excluded — overlaps with customer_id and is unreliable for detail pages
--   seller_customer_id: excluded — SRP-only UUID field, not present on detail pages

select
    vin17,
    listing_id,
    artifact_id,
    fetched_at,
    md5(concat_ws('|',
        coalesce(listing_id,                       ''),
        coalesce(vin17,                            ''),
        coalesce(cast(price       as varchar),     ''),
        coalesce(cast(mileage     as varchar),     ''),
        coalesce(cast(msrp        as varchar),     ''),
        coalesce(make,                             ''),
        coalesce(model,                            ''),
        coalesce(vehicle_trim,                     ''),
        coalesce(cast(model_year  as varchar),     ''),
        coalesce(stock_type,                       ''),
        coalesce(fuel_type,                        ''),
        coalesce(body_style,                       ''),
        coalesce(listing_state,                    ''),
        coalesce(dealer_name,                      ''),
        coalesce(dealer_zip,                       ''),
        coalesce(dealer_city,                      ''),
        coalesce(dealer_state,                     ''),
        coalesce(customer_id,                      '')
    ))                          as parsed_fingerprint,
    price,
    mileage,
    listing_state
from {{ ref('stg_observations') }}
where source = 'detail'
  and vin17 is not null
