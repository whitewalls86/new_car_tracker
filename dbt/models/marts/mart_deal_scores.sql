-- Deal scores for all active listings.
-- One row per VIN, ranked by composite deal_score (0-100).

with active_vins as (
    -- VINs seen in SRP within last 3 days
    select distinct vin17 as vin
    from {{ ref('stg_srp_observations') }}
    where vin17 is not null
      and fetched_at >= now() - interval '{{ var("staleness_window_days") }} days'
),

-- Check if VIN was seen locally in last 3 days
local_seen as (
    select distinct s.vin17 as vin
    from {{ ref('stg_srp_observations') }} s
    inner join {{ source('public', 'raw_artifacts') }} ra
        on ra.artifact_id = s.artifact_id
    where s.vin17 is not null
      and ra.search_scope = 'local'
      and s.fetched_at >= now() - interval '{{ var("staleness_window_days") }} days'
),

scored as (
    select
        -- Identity
        a.vin,
        v.listing_id,
        a.make,
        a.model,
        a.vehicle_trim,
        a.model_year,
        a.fuel_type,
        a.body_style,
        a.stock_type,
        a.search_key,
        a.canonical_detail_url,

        -- Seller
        a.seller_customer_id,
        a.seller_zip,
        -- dealer_name: prefer detail parse, fallback to dealers table (now joined via numeric customer_id)
        coalesce(ldn.dealer_name, dlr.name) as dealer_name,

        dlr.city as dealer_city,
        dlr.state as dealer_state,
        dlr.phone as dealer_phone,
        dlr.rating as dealer_rating,

        -- Price
        v.price as current_price,
        v.price_observed_at,
        v.price_source,
        a.msrp,

        -- MSRP discount
        case when a.msrp > 0 and v.price > 0
             then round((a.msrp - v.price)::numeric / a.msrp * 100, 2)
             else null
        end as msrp_discount_pct,
        case when a.msrp > 0 and v.price > 0
             then a.msrp - v.price
             else null
        end as msrp_discount_amt,

        -- Days on market
        dom.first_seen_at,
        dom.last_seen_at,
        dom.first_seen_local_at,
        dom.days_on_market,
        dom.days_observed,

        -- Price history
        ph.first_price,
        ph.min_price,
        ph.max_price,
        ph.price_drop_count,
        ph.price_increase_count,
        ph.total_price_observations,
        case when ph.first_price > 0 and v.price > 0
             then round((ph.first_price - v.price)::numeric / ph.first_price * 100, 2)
             else null
        end as total_price_drop_pct,

        -- National benchmarks (for the VIN's make/model/trim)
        b.national_listing_count,
        b.national_avg_price,
        b.national_median_price,
        b.national_p10_price,
        b.national_p25_price,
        b.national_avg_discount_pct,

        -- National price percentile (0 = cheapest, 1 = most expensive)
        coalesce(pctl.national_price_percentile, 0.75) as national_price_percentile,


        -- Dealer inventory
        coalesce(di.dealer_inventory_count, 0) as dealer_inventory_count,

        -- Scope flags
        (ls.vin is not null) as is_local,

        -- Listing state (from detail scrapes, null if SRP-only)
        v.listing_state,

        -- ===== DEAL SCORE (0-100) =====
        round((
            -- MSRP discount (35 pts): 10%+ discount = full points
            coalesce(
                greatest(0, least(35,
                    (a.msrp - v.price)::numeric / nullif(a.msrp, 0) * 350
                )), 0)

            -- National price percentile (30 pts): lower percentile = better
            + (1 - coalesce(pctl.national_price_percentile, 0.75)) * 30

            -- Days on market (15 pts): capped at 90 days
            + least(coalesce(dom.days_on_market, 0), 90) / 90.0 * 15

            -- Price drops (10 pts): capped at 3 drops
            + least(coalesce(ph.price_drop_count, 0), 3) / 3.0 * 10

            -- Dealer inventory depth (5 pts): capped at 10 units
            + least(coalesce(di.dealer_inventory_count, 0), 10) / 10.0 * 5

            -- National supply (5 pts): capped at 500 listings
            + least(coalesce(b.national_listing_count, 0), 500) / 500.0 * 5
        )::numeric, 1) as deal_score

    from active_vins av
    inner join {{ ref('int_vehicle_attributes') }} a on a.vin = av.vin
    inner join {{ ref('mart_vehicle_snapshot') }} v on v.vin = av.vin
    left join {{ ref('int_listing_days_on_market') }} dom on dom.vin = av.vin
    left join {{ ref('int_price_history_by_vin') }} ph on ph.vin = av.vin
    left join {{ ref('int_model_price_benchmarks') }} b
        on b.make = a.make and b.model = a.model
        and ((b.vehicle_trim = a.vehicle_trim) or (b.vehicle_trim is null and a.vehicle_trim is null))
    left join {{ ref('int_dealer_inventory') }} di
        on di.seller_customer_id = a.seller_customer_id
        and di.make = a.make and di.model = a.model
    left join {{ ref('int_price_percentiles_by_vin') }} pctl on pctl.vin = av.vin
    left join {{ source('public', 'dealers') }} dlr
        on dlr.customer_id = v.customer_id
    left join {{ ref('int_latest_dealer_name_by_vin') }} ldn on ldn.vin = av.vin
    left join local_seen ls on ls.vin = av.vin
    where v.price is not null and v.price > 0
)

select
    *,
    case
        when deal_score >= 70 then 'excellent'
        when deal_score >= 50 then 'good'
        when deal_score >= 30 then 'fair'
        else 'weak'
    end as deal_tier
from scored
