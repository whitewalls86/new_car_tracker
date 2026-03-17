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
      and s.fetched_at >= now() - interval '{{ var("staleness_window_days") }} days'
),

deduped as (
    select * from active_listings where rn = 1
)

select
    d.seller_customer_id,
    d.seller_zip,
    dlr.name as dealer_name,
    dlr.city as dealer_city,
    dlr.state as dealer_state,
    d.make,
    d.model,
    count(distinct d.vin) as dealer_inventory_count
from deduped d
left join {{ source('public', 'dealers') }} dlr
    on dlr.customer_id = d.seller_customer_id
group by d.seller_customer_id, d.seller_zip, dlr.name, dlr.city, dlr.state, d.make, d.model
