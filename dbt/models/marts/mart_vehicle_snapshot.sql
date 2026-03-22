select
    t.vin,

    -- Tier-1 state
    t.listing_id,
    -- Infer listing_state for SRP-only VINs: if seen within 7 days treat as active,
    -- otherwise treat as unlisted (only detail scrapes provide a confirmed state)
    case
        when t.listing_state is not null then t.listing_state
        when t.observed_at >= now() - interval '7 days' then 'active'
        else 'unlisted'
    end as listing_state,
    t.mileage,
    t.observed_at        as tier1_observed_at,
    t.artifact_id        as tier1_artifact_id,
    t.source             as tier1_source,

    -- Tier-1 winner fetch metadata (THIS is what ops should use)
    t.canonical_detail_url as tier1_canonical_detail_url,
    t.seller_customer_id   as tier1_seller_customer_id,
    t.customer_id,

    -- Convenience alias
    t.canonical_detail_url as current_listing_url,

    -- Price
    p.price,
    p.price_observed_at,
    p.price_artifact_id,
    p.price_source,
    p.price_tier

from {{ ref('int_latest_tier1_observation_by_vin') }} t
inner join {{ ref('int_vehicle_attributes') }} a
  on a.vin = t.vin
inner join {{ ref('int_scrape_targets') }} tgt
  on tgt.make = a.make and tgt.model = a.model
left join {{ ref('int_latest_price_by_vin') }} p
  on p.vin = t.vin
