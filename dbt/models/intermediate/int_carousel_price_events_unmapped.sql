{{ config(materialized='table') }}

with hints as (
    select
        h.artifact_id,
        h.fetched_at as observed_at,
        h.listing_id,
        h.price,
        lower(split_part(h.body, ' ', 3)) as hint_make
    from {{ ref('stg_detail_carousel_hints') }} h
    where h.price is not null
      and h.price > 0
      and h.body is not null
),

-- Only keep hints whose make matches an active search config
active_makes as (
    select distinct jsonb_array_elements_text(params->'makes') as make
    from {{ source('public', 'search_configs') }}
    where enabled = true
),

unmapped as (
    select
        m.vin,
        h.listing_id,
        h.artifact_id,
        h.observed_at,
        h.price
    from hints h
    inner join active_makes am
      on am.make = h.hint_make
    left join {{ ref('int_listing_to_vin') }} m
      on m.listing_id = h.listing_id
    where m.listing_id IS NULL
)

select
    listing_id,
    artifact_id,
    observed_at,
    price,
    'detail_carousel'::text as source,
    2::int as tier
from unmapped
