{{
  config(materialized='table')
}}

-- Deal scores for all active listings. One row per VIN.
-- Scored 0-100 across four factors: MSRP discount, national price percentile,
-- days on market, and price drop history.

with

-- National price percentile per VIN within its make/model
price_percentiles as (
    select
        v.vin,
        percent_rank() over (
            partition by v.make, v.model
            order by v.price
        ) as national_price_percentile
    from {{ ref('mart_vehicle_snapshot') }} v
    where v.price is not null and v.price > 0
),

-- Dealer inventory depth: how many tracked VINs at the same dealer, same make/model
dealer_inventory as (
    select
        customer_id,
        make,
        model,
        count(*) as dealer_inventory_count
    from {{ ref('mart_vehicle_snapshot') }}
    where listing_state = 'active'
      and customer_id is not null
    group by customer_id, make, model
),

scored as (
    select
        -- Identity
        v.vin,
        v.listing_id,
        v.make,
        v.model,
        v.vehicle_trim,
        v.model_year,
        v.fuel_type,
        v.body_style,
        v.stock_type,
        v.canonical_detail_url,
        v.listing_state,

        -- Dealer
        v.customer_id,
        v.dealer_zip,
        coalesce(d.name, v.dealer_name)  as dealer_name,

        -- Price
        v.price                          as current_price,
        v.price_observed_at,
        v.msrp,

        -- MSRP discount
        case when v.msrp > 0 and v.price > 0
             then round((v.msrp - v.price)::numeric / v.msrp * 100, 2)
        end                              as msrp_discount_pct,
        case when v.msrp > 0 and v.price > 0
             then v.msrp - v.price
        end                              as msrp_discount_amt,

        -- Time on market
        v.first_seen_at,
        v.last_seen_at,
        v.days_on_market,

        -- Price history
        v.first_price,
        v.min_price,
        v.max_price,
        v.price_drop_count,
        v.price_increase_count,
        v.total_price_observations,
        case when v.first_price > 0 and v.price > 0
             then round((v.first_price - v.price)::numeric / v.first_price * 100, 2)
        end                              as total_price_drop_pct,

        -- National benchmarks
        b.national_listing_count,
        b.national_avg_price,
        b.national_median_price,
        b.national_p10_price,
        b.national_p25_price,
        b.national_avg_discount_pct,

        -- National price percentile (0 = cheapest, 1 = most expensive)
        coalesce(pctl.national_price_percentile, 0.75) as national_price_percentile,

        -- Dealer inventory depth
        coalesce(di.dealer_inventory_count, 0)         as dealer_inventory_count,

        -- ===== DEAL SCORE (0-100) =====
        round((
            -- MSRP discount (35 pts): 10%+ discount = full points
            coalesce(greatest(0, least(35,
                (v.msrp - v.price)::numeric / nullif(v.msrp, 0) * 350
            )), 0)

            -- National price percentile (30 pts): lower = better deal
            + (1 - coalesce(pctl.national_price_percentile, 0.75)) * 30

            -- Days on market (15 pts): capped at 90 days
            + least(coalesce(v.days_on_market, 0), 90) / 90.0 * 15

            -- Price drops (10 pts): capped at 3 drops
            + least(coalesce(v.price_drop_count, 0), 3) / 3.0 * 10

            -- Dealer inventory depth (5 pts): capped at 10 units
            + least(coalesce(di.dealer_inventory_count, 0), 10) / 10.0 * 5

            -- National supply (5 pts): capped at 500 listings
            + least(coalesce(b.national_listing_count, 0), 500) / 500.0 * 5
        )::numeric, 1)                                 as deal_score

    from {{ ref('mart_vehicle_snapshot') }} v
    left join {{ ref('int_benchmarks') }} b
        on b.make = v.make and b.model = v.model
    left join price_percentiles pctl
        on pctl.vin = v.vin
    left join dealer_inventory di
        on di.customer_id = v.customer_id
        and di.make = v.make and di.model = v.model
    left join {{ ref('stg_dealers') }} d
        on d.customer_id = v.customer_id
    where v.listing_state = 'active'
      and v.price is not null
      and v.price > 0
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
