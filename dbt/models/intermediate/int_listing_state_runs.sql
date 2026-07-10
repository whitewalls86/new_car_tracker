{{
  config(
    materialized='incremental',
    unique_key='vin17',
    incremental_strategy='delete+insert'
  )
}}

-- Gaps-and-islands: collapses consecutive identical parsed_fingerprints into
-- contiguous runs, partitioned by vin17.
-- listing_id is a safe group key because it is included in the fingerprint hash —
-- a listing_id change always produces a new fingerprint and therefore a new run.
-- listing_state is constant within a run (it's in the hash) and is carried forward
-- to avoid a range-join back to int_listing_state_fingerprints in downstream models.
--
-- Incremental strategy: affected-VIN replacement (Plan 123 Phase 4), same
-- delete+insert base strategy as int_listing_state_fingerprints (Phase 2) and
-- int_price_history (Phase 3). unique_key='vin17' here is an ENTITY
-- REPLACEMENT KEY, not a row-unique key — the output grain is multiple runs
-- per vin17. delete+insert with this unique_key deletes ALL existing target
-- rows for each affected vin17 and reinserts its complete recomputed run
-- history, so do not add a `unique` data test on vin17.
--
-- A late-arriving or corrected fingerprint can split a previously single run
-- into two, or collapse what looked like two runs into one — both require
-- recomputing gaps-and-islands over that VIN's ENTIRE fingerprint history, not
-- just the new rows. On an incremental run, only vin17s with a fingerprint
-- inside listing_state_runs_incremental_lookback_days of the target's
-- max(run_ended_at) are treated as affected, but this model then reads ALL of
-- that vin17's history from int_listing_state_fingerprints (itself
-- incrementally but fully maintained) to recompute every run.

with affected_vins as (

    select distinct vin17
    from {{ ref('int_listing_state_fingerprints') }}

    {% if is_incremental() %}
    where fetched_at >= (
        select coalesce(max(run_ended_at), timestamp '1900-01-01')
               - interval '{{ var("listing_state_runs_incremental_lookback_days", 3) }}' day
        from {{ this }}
    )
    {% endif %}

),

ordered as (
    select
        f.vin17,
        f.listing_id,
        f.artifact_id,
        f.fetched_at,
        f.parsed_fingerprint,
        f.listing_state,
        lag(f.parsed_fingerprint) over (
            partition by f.vin17 order by f.fetched_at, f.artifact_id
        ) as prev_fingerprint
    from {{ ref('int_listing_state_fingerprints') }} f
    {% if is_incremental() %}
    inner join affected_vins av on av.vin17 = f.vin17
    {% endif %}
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
