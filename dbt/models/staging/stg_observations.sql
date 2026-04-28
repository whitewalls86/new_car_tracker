{{
  config(materialized='view')
}}

-- Unified view of all parsed observations from MinIO silver.
-- Single source of truth for all three observation types (srp, detail, carousel).
-- Adds vin17: cleaned, validated 17-character VIN. All other fields pass through.

select
    artifact_id,
    listing_id,
    source,
    listing_state,
    fetched_at,
    vin,
    case
        when vin is not null
         and length(vin) = 17
         and regexp_matches(upper(vin), '^[A-Z0-9]{17}$')
        then upper(vin)
        else null
    end                  as vin17,
    price,
    make,
    model,
    trim                 as vehicle_trim,
    year                 as model_year,
    mileage,
    msrp,
    stock_type,
    fuel_type,
    body_style,
    dealer_name,
    dealer_zip,
    customer_id,
    canonical_detail_url,
    financing_type,
    seller_zip,
    seller_customer_id,
    page_number,
    position_on_page,
    trid,
    isa_context
from {{ source('silver', 'observations') }}
