-- ops_detail_scrape_queue: all vehicles eligible for a detail scrape, priority-ordered.
--
-- Pool 1 (priority=1): Stale VINs — one per dealer, stalest first
-- Pool 2 (priority=2): Force-stale VINs — >36 h old, not already picked in pool 1
-- Pool 3 (priority=3): Unmapped carousel hints — newest first, fills remaining capacity
--
-- Consumers: n8n "Scrape Detail Pages" workflow queries this view,
-- filters out active claims, and LIMITs to the desired batch size.

with stale as (
    select
        ovs.vin,
        ovs.current_listing_url,
        ovs.listing_id,
        ovs.tier1_seller_customer_id as seller_customer_id,
        ovs.is_price_stale,
        ovs.is_full_details_stale,
        ovs.stale_reason,
        ovs.tier1_age_hours,

        row_number() over (
            partition by coalesce(ovs.tier1_seller_customer_id::text, ovs.vin)
            order by
                case when ovs.is_full_details_stale then 0 else 1 end,
                coalesce(ovs.price_observed_at, 'epoch'::timestamptz) asc,
                coalesce(ovs.tier1_observed_at, 'epoch'::timestamptz) asc
        ) as dealer_row_num

    from {{ ref('ops_vehicle_staleness') }} ovs
    where (ovs.is_price_stale or ovs.is_full_details_stale)
      and coalesce(ovs.listing_state, 'active') = 'active'
      and ovs.current_listing_url is not null
),

-- Pool 1: one per dealer (existing logic)
dealer_picks as (
    select
        vin,
        current_listing_url,
        listing_id,
        seller_customer_id,
        stale_reason,
        1 as priority
    from stale
    where dealer_row_num = 1
),

-- Pool 2: force-grab vehicles >36h stale that dealer_picks missed
force_stale as (
    select
        vin,
        current_listing_url,
        listing_id,
        seller_customer_id,
        'force_stale_36h' as stale_reason,
        2 as priority
    from stale
    where tier1_age_hours > 36
      and dealer_row_num > 1
),

-- Pool 3: unmapped carousel hints (newest first)
carousel as (
    select
        listing_id as vin,
        'https://www.cars.com/vehicledetail/' || listing_id || '/' as current_listing_url,
        listing_id,
        null::text as seller_customer_id,
        'unmapped_carousel' as stale_reason,
        3 as priority
    from (
        select
            listing_id,
            row_number() over (partition by listing_id order by observed_at desc) as rn
        from {{ ref('int_carousel_price_events_unmapped') }}
    ) sub
    where rn = 1
),

combined as (
    select * from dealer_picks
    union all
    select * from force_stale
    union all
    select * from carousel
)

select distinct on (listing_id)
    vin,
    current_listing_url,
    listing_id,
    seller_customer_id,
    stale_reason,
    priority
from combined
order by listing_id, priority
