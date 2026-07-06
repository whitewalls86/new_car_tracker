{{
  config(materialized='table')
}}

-- Gaps-and-islands: collapses consecutive identical parsed_fingerprints into
-- contiguous runs, partitioned by vin17.
-- listing_id is a safe group key because it is included in the fingerprint hash —
-- a listing_id change always produces a new fingerprint and therefore a new run.
-- listing_state is constant within a run (it's in the hash) and is carried forward
-- to avoid a range-join back to int_listing_state_fingerprints in downstream models.

with ordered as (
    select
        vin17,
        listing_id,
        artifact_id,
        fetched_at,
        parsed_fingerprint,
        listing_state,
        lag(parsed_fingerprint) over (
            partition by vin17 order by fetched_at, artifact_id
        ) as prev_fingerprint
    from {{ ref('int_listing_state_fingerprints') }}
),

flagged as (
    select
        *,
        case
            when prev_fingerprint is null
              or parsed_fingerprint != prev_fingerprint
            then 1
            else 0
        end as is_new_run
    from ordered
),

numbered as (
    select
        *,
        sum(is_new_run) over (
            partition by vin17
            order by fetched_at, artifact_id
            rows between unbounded preceding and current row
        ) as run_id
    from flagged
),

collapsed as (
    select
        vin17,
        listing_id,
        parsed_fingerprint,
        run_id,
        min(fetched_at)    as run_started_at,
        max(fetched_at)    as run_ended_at,
        count(*)           as artifact_count,
        min(listing_state) as listing_state
    from numbered
    group by vin17, listing_id, parsed_fingerprint, run_id
),

with_lead as (
    select
        *,
        lead(run_started_at) over (
            partition by vin17 order by run_started_at, run_id
        ) as next_state_started_at
    from collapsed
)

select
    vin17,
    listing_id,
    parsed_fingerprint,
    listing_state,
    run_started_at,
    run_ended_at,
    artifact_count,
    datediff('hour', run_started_at, run_ended_at)        as run_duration_hours,
    next_state_started_at,
    datediff('hour', run_ended_at, next_state_started_at) as hours_until_change,
    next_state_started_at is null                         as is_open_run
from with_lead
