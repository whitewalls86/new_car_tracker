{{ config(materialized='table') }}

with hints as (
    select
        h.artifact_id,
        h.fetched_at as observed_at,
        h.listing_id,
        h.price,
        h.body
    from {{ ref('stg_detail_carousel_hints') }} h
    where h.price is not null
      and h.price > 0
      and h.body is not null
),

-- Only keep hints whose make AND model match an active scrape target.
-- Body format: "{condition} {year} {Make} {Model...} {Trim...}"
-- e.g. "Certified 2021 Ford Escape SE", "New 2025 Toyota RAV4 Hybrid"
filtered as (
    select
        h.artifact_id,
        h.observed_at,
        h.listing_id,
        h.price
    from hints h
    inner join {{ ref('int_scrape_targets') }} t
        on h.body ilike '% ' || t.make || ' ' || t.model || '%'
),

unmapped as (
    select
        m.vin,
        f.listing_id,
        f.artifact_id,
        f.observed_at,
        f.price
    from filtered f
    left join {{ ref('int_listing_to_vin') }} m
      on m.listing_id = f.listing_id
    left join {{ ref('stg_detail_observations') }} d
      on f.listing_id = d.listing_id
    where m.listing_id IS NULL AND d.vin IS NULL
)

select
    listing_id,
    artifact_id,
    observed_at,
    price,
    'detail_carousel'::text as source,
    2::int as tier
from unmapped
