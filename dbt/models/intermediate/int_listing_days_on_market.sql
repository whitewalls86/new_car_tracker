-- Days on market per VIN — tracks first/last seen across all sources
-- (SRP, detail observations, and carousel hints via listing_id→VIN mapping).
-- Incremental: on each run, only scan new observations and merge with existing aggregates.

{{ config(
    materialized='incremental',
    unique_key='vin',
    incremental_strategy='merge'
) }}

with srp_obs as (
    select
        s.vin17 as vin,
        s.fetched_at,
        ra.search_scope
    from {{ ref('stg_srp_observations') }} s
    inner join {{ source('public', 'raw_artifacts') }} ra
        on ra.artifact_id = s.artifact_id
    where s.vin17 is not null
    {% if is_incremental() %}
      and s.fetched_at > (select max(last_seen_at) - interval '1 hour' from {{ this }})
    {% endif %}
),

detail_obs as (
    select
        d.vin17 as vin,
        d.fetched_at
    from {{ ref('stg_detail_observations') }} d
    where d.vin17 is not null
    {% if is_incremental() %}
      and d.fetched_at > (select max(last_seen_at) - interval '1 hour' from {{ this }})
    {% endif %}
),

carousel_obs as (
    select
        m.vin,
        h.fetched_at
    from {{ ref('stg_detail_carousel_hints') }} h
    inner join {{ ref('int_listing_to_vin') }} m
        on m.listing_id = h.listing_id
    {% if is_incremental() %}
    where h.fetched_at > (select max(last_seen_at) - interval '1 hour' from {{ this }})
    {% endif %}
),

-- Scope-specific aggregates from new SRP data only
new_srp_agg as (
    select
        vin,
        min(case when search_scope = 'national' then fetched_at end) as first_seen_national_at,
        min(case when search_scope = 'local' then fetched_at end) as first_seen_local_at,
        max(case when search_scope = 'local' then fetched_at end) as last_seen_local_at
    from srp_obs
    group by vin
),

-- All new observations combined
new_obs as (
    select vin, fetched_at from srp_obs
    union all
    select vin, fetched_at from detail_obs
    union all
    select vin, fetched_at from carousel_obs
),

new_overall as (
    select
        vin,
        min(fetched_at) as first_seen_at,
        max(fetched_at) as last_seen_at,
        count(distinct fetched_at::date) as days_observed
    from new_obs
    group by vin
)

{% if is_incremental() %}

select
    n.vin,
    least(n.first_seen_at, coalesce(e.first_seen_at, n.first_seen_at)) as first_seen_at,
    greatest(n.last_seen_at, coalesce(e.last_seen_at, n.last_seen_at)) as last_seen_at,
    least(s.first_seen_national_at, e.first_seen_national_at) as first_seen_national_at,
    least(s.first_seen_local_at, e.first_seen_local_at) as first_seen_local_at,
    greatest(s.last_seen_local_at, e.last_seen_local_at) as last_seen_local_at,
    extract(day from now() - least(n.first_seen_at, coalesce(e.first_seen_at, n.first_seen_at)))::int as days_on_market,
    -- Approximate: existing days + new distinct days (may slightly overcount)
    coalesce(e.days_observed, 0) + n.days_observed as days_observed
from new_overall n
left join {{ this }} e on e.vin = n.vin
left join new_srp_agg s on s.vin = n.vin

{% else %}

select
    o.vin,
    o.first_seen_at,
    o.last_seen_at,
    s.first_seen_national_at,
    s.first_seen_local_at,
    s.last_seen_local_at,
    extract(day from now() - o.first_seen_at)::int as days_on_market,
    o.days_observed
from new_overall o
left join new_srp_agg s on s.vin = o.vin

{% endif %}
