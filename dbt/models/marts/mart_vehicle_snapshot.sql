{{
  config(materialized='table')
}}

-- Current state snapshot: one row per VIN.
-- Separates two concerns:
--   1. listing_state  — from the most recent observation per VIN, any source,
--      any data completeness. Unlisted Cars.com detail pages don't return vehicle
--      JSON (VIN/make/model are NULL), but the listing_state they carry is still
--      authoritative and must override a stale "active" signal from a prior SRP hit.
--   2. vehicle attrs  — from int_latest_observation, which requires make IS NOT NULL,
--      so it falls back to the best prior SRP/detail obs that had full vehicle data.
--
-- listing_state coalesce order:
--   1. If the most recent observation came from source='detail', trust its listing_state.
--   2. Otherwise infer: seen on SRP within 7 days → 'active', else → 'unlisted'.

with latest_state as (
    -- Most recent observation per VIN regardless of data completeness.
    -- Used only for listing_state; vehicle attributes come from int_latest_observation.
    select distinct on (vin17)
        vin17,
        listing_state,
        source       as state_source
    from {{ ref('stg_observations') }}
    where vin17 is not null
    order by vin17, fetched_at desc
)

select
    obs.vin17                   as vin,
    obs.listing_id,
    coalesce(
        case when ls.state_source = 'detail' then ls.listing_state end,
        case when ph.last_seen_at >= now() - interval '7 days' then 'active'
             else 'unlisted'
        end
    )                           as listing_state,

    -- Vehicle attributes (from best data-rich observation — make IS NOT NULL)
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
left join latest_state ls
    on ls.vin17 = obs.vin17
inner join {{ ref('int_active_make_models') }} amm
    on amm.make  = lower(obs.make)
   and amm.model = lower(obs.model)
