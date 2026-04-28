{{
  config(materialized='table')
}}

-- Current state snapshot: one row per VIN.
-- Combines latest observation attributes with current price and price history.
-- listing_state: 'detail' observations are authoritative; SRP-only VINs are
-- inferred active if seen within 7 days, otherwise unlisted.

select
    obs.vin17                   as vin,
    obs.listing_id,
    coalesce(
        case when obs.source = 'detail' then obs.listing_state end,
        case when ph.last_seen_at >= now() - interval '7 days' then 'active'
             else 'unlisted'
        end
    )                           as listing_state,

    -- Vehicle attributes (from best available observation)
    obs.make,
    obs.model,
    obs.vehicle_trim,
    obs.model_year,
    obs.mileage,
    obs.msrp,
    obs.fuel_type,
    obs.body_style,
    obs.stock_type,
    obs.canonical_detail_url,

    -- Dealer linkage
    obs.customer_id,
    obs.seller_customer_id,
    obs.dealer_name,
    obs.dealer_zip,

    -- Observation freshness
    obs.fetched_at              as last_observed_at,

    -- Price (current + history from event stream)
    ph.current_price            as price,
    ph.price_observed_at,
    ph.first_price,
    ph.min_price,
    ph.max_price,
    ph.price_drop_count,
    ph.price_increase_count,
    ph.total_price_observations,

    -- Time on market
    ph.first_seen_at,
    ph.last_seen_at,
    ph.days_on_market

from {{ ref('int_latest_observation') }} obs
left join {{ ref('int_price_history') }} ph
    on ph.vin = obs.vin17
