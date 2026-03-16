-- Price trajectory per VIN — first price, drops, min/max.

with ordered as (
    select
        vin,
        observed_at,
        price,
        lag(price) over (partition by vin order by observed_at, artifact_id) as prev_price,
        row_number() over (partition by vin order by observed_at, artifact_id) as obs_num
    from {{ ref('int_price_events') }}
    where price is not null and price > 0
)

select
    vin,
    min(case when obs_num = 1 then price end) as first_price,
    min(case when obs_num = 1 then observed_at end) as first_price_observed_at,
    min(price) as min_price,
    max(price) as max_price,
    count(case when prev_price is not null and price < prev_price then 1 end) as price_drop_count,
    count(case when prev_price is not null and price > prev_price then 1 end) as price_increase_count,
    count(*) as total_price_observations
from ordered
group by vin
