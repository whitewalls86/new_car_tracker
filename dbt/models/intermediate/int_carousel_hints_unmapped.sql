with hints as (
    select
        h.artifact_id,
        h.fetched_at as observed_at,
        h.listing_id,
        h.price
    from {{ ref('stg_detail_carousel_hints') }} h
    where h.price is not null
      and h.price > 0
),

joined as (
    select
        h.*,
        m.vin
    from hints h
    left join {{ ref('int_listing_to_vin') }} m
      on m.listing_id = h.listing_id
)

select
    listing_id,
    artifact_id,
    observed_at,
    price
from joined
where vin is null
