-- Days on market per VIN — tracks first/last seen across all scopes.

with obs as (
    select
        s.vin17 as vin,
        s.fetched_at,
        ra.search_scope
    from {{ ref('stg_srp_observations') }} s
    inner join {{ source('public', 'raw_artifacts') }} ra
        on ra.artifact_id = s.artifact_id
    where s.vin17 is not null
)

select
    vin,
    min(fetched_at) as first_seen_at,
    max(fetched_at) as last_seen_at,
    min(case when search_scope = 'national' then fetched_at end) as first_seen_national_at,
    min(case when search_scope = 'local' then fetched_at end) as first_seen_local_at,
    max(case when search_scope = 'local' then fetched_at end) as last_seen_local_at,
    extract(day from now() - min(fetched_at))::int as days_on_market,
    count(distinct fetched_at::date) as days_observed
from obs
group by vin
