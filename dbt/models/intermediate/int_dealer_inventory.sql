-- Dealer inventory counts — how many active units of each model a dealer has.
-- Higher counts signal more negotiation leverage.

with active_listings as (
    select
        s.vin17 as vin,
        s.seller_customer_id,
        s.seller_zip,
        s.make,
        s.model,
        row_number() over (
            partition by s.vin17
            order by s.fetched_at desc, s.artifact_id desc
        ) as rn
    from {{ ref('stg_srp_observations') }} s
    where s.vin17 is not null
      and s.seller_customer_id is not null
      and s.fetched_at >= now() - interval '3 days'
),

deduped as (
    select * from active_listings where rn = 1
)

select
    seller_customer_id,
    seller_zip,
    make,
    model,
    count(distinct vin) as dealer_inventory_count
from deduped
group by seller_customer_id, seller_zip, make, model
