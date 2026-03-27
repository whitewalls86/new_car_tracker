{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'merge'
  )
}}

with hints as (
    select
        h.id,
        h.artifact_id,
        h.fetched_at as observed_at,
        h.listing_id,
        h.price,
        -- Parse make/model from body: "{condition} {year} {Make} {Model...}"
        -- e.g. "Certified 2021 Ford Escape SE", "New 2025 Toyota RAV4 Hybrid"
        (string_to_array(h.body, ' '))[3] as parsed_make,
        (string_to_array(h.body, ' '))[4] as parsed_model
    from {{ ref('stg_detail_carousel_hints') }} h
    where h.price is not null
      and h.price > 0
      and h.body is not null
    {% if is_incremental() %}
      and h.id > (select coalesce(max(id), 0) from {{ this }})
    {% endif %}
)

select
    h.id,
    h.artifact_id,
    h.observed_at,
    h.listing_id,
    h.price,
    case when t.make is not null then true else false end as is_valid_target
from hints h
left join {{ ref('int_scrape_targets') }} t
    on lower(h.parsed_make) = lower(t.make)
    and lower(h.parsed_model) = lower(t.model)
