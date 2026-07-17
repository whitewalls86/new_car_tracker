{{
  config(materialized='ephemeral' if target.type == 'spark' else 'view')
}}

-- Unified view of all parsed observations from MinIO silver.
-- Single source of truth for all three observation types (srp, detail, carousel).
-- Adds vin17: cleaned, validated 17-character VIN. All other fields pass through.
--
-- Plan 125 Gate B: `ephemeral` on the spark target, `view` everywhere else.
-- Not a semantic difference -- both mean "no stored data, recomputed on demand".
-- A persisted Spark view stores its body and re-analyzes it against the VIEW's
-- own catalog on read, which rewrites the parquet.`s3a://...` reference below
-- into cartracker.parquet.`s3a://...` and fails with TABLE_OR_VIEW_NOT_FOUND.
-- ephemeral compiles to a CTE, where the reference resolves correctly.
-- (Found at Gate A on stg_blocked_cooldown_events; same cause here.)

select
    artifact_id,
    listing_id,
    source,
    listing_state,
    fetched_at,
    written_at,
    vin,
    case
        when vin is not null
         and length(vin) = 17
         and {{ regex_matches('upper(vin)', '^[A-Z0-9]{17}$') }}
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
    dealer_city,
    dealer_state,
    customer_id,
    canonical_detail_url,
    financing_type,
    seller_zip,
    seller_customer_id,
    page_number,
    position_on_page,
    trid,
    isa_context,
    body,
    condition
from {{ parquet_source('silver', 'observations') }}
