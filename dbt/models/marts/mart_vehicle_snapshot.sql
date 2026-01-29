select
    t.vin,

    -- Tier-1 state
    t.listing_id,
    t.listing_state,
    t.mileage,
    t.observed_at        as tier1_observed_at,
    t.artifact_id        as tier1_artifact_id,
    t.source             as tier1_source,

    -- Price
    p.price,
    p.price_observed_at,
    p.price_artifact_id,
    p.price_source,
    p.price_tier

from {{ ref('int_latest_tier1_observation_by_vin') }} t
left join {{ ref('int_latest_price_by_vin') }} p
  on p.vin = t.vin
