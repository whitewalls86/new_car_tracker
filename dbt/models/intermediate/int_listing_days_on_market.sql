-- Days on market per VIN — tracks first/last seen across all sources
-- (SRP, detail observations, and carousel hints via listing_id→VIN mapping).

with srp_obs as (
    select
        s.vin17 as vin,
        s.fetched_at,
        ra.search_scope
    from {{ ref('stg_srp_observations') }} s
    inner join {{ source('public', 'raw_artifacts') }} ra
        on ra.artifact_id = s.artifact_id
    where s.vin17 is not null
),

detail_obs as (
    select
        d.vin17 as vin,
        d.fetched_at
    from {{ ref('stg_detail_observations') }} d
    where d.vin17 is not null
),

carousel_obs as (
    select
        m.vin,
        h.fetched_at
    from {{ ref('stg_detail_carousel_hints') }} h
    inner join {{ ref('int_listing_to_vin') }} m
        on m.listing_id = h.listing_id
),

-- Scope-specific aggregates from SRP only
srp_agg as (
    select
        vin,
        min(case when search_scope = 'national' then fetched_at end) as first_seen_national_at,
        min(case when search_scope = 'local' then fetched_at end) as first_seen_local_at,
        max(case when search_scope = 'local' then fetched_at end) as last_seen_local_at
    from srp_obs
    group by vin
),

-- All sources combined for overall first/last seen
all_obs as (
    select vin, fetched_at from srp_obs
    union all
    select vin, fetched_at from detail_obs
    union all
    select vin, fetched_at from carousel_obs
),

overall as (
    select
        vin,
        min(fetched_at) as first_seen_at,
        max(fetched_at) as last_seen_at,
        extract(day from now() - min(fetched_at))::int as days_on_market,
        count(distinct fetched_at::date) as days_observed
    from all_obs
    group by vin
)

select
    o.vin,
    o.first_seen_at,
    o.last_seen_at,
    s.first_seen_national_at,
    s.first_seen_local_at,
    s.last_seen_local_at,
    o.days_on_market,
    o.days_observed
from overall o
left join srp_agg s on s.vin = o.vin
