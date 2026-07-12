{{
  config(
    materialized='incremental',
    unique_key='listing_id',
    incremental_strategy='delete+insert'
  )
}}

-- Gaps-and-islands over int_listing_observation_fingerprints (all-source:
-- detail, SRP, carousel), collapsing consecutive identical observation states
-- into contiguous runs partitioned by listing_id.
--
-- This is NOT the detail-only int_listing_state_runs model. That model
-- partitions by vin17 because every one of its rows has a resolved VIN by
-- construction (its source is detail-only, vin17-required
-- int_listing_state_fingerprints). Here vin17 is frequently null (SRP/carousel
-- rows commonly lack a resolved VIN), so vin17 cannot be the partition key
-- without silently dropping or mis-grouping those rows. listing_id is required
-- (not null) in int_listing_observation_fingerprints, so it is the reliable
-- partition key for all-source observation cadence. vin17 is carried forward
-- as any resolved VIN seen within the run (max(vin17), arbitrary but
-- deterministic) purely for convenience joins downstream — it is not this
-- model's grain.
--
-- Run boundary: this model does NOT reuse int_listing_observation_fingerprints'
-- parsed_fingerprint for gaps-and-islands, because that hash intentionally
-- includes `source` (Plan 123 Phase 2b — it disambiguates artifact_id+listing_id
-- keys, which is correct for that model's purpose). Reusing it here would open
-- a new "run" every time the observing source changes — e.g. a detail scrape
-- followed by an SRP re-observation of the identical price/state — which is not
-- a real cadence signal, just a source alternation. Instead, run boundaries are
-- computed here from observation_state_key, a hash of only the business-state
-- fields int_listing_observation_fingerprints actually exposes as plain columns
-- (price, mileage, listing_state), independent of source. A run therefore
-- represents a period where the listing's observed price/mileage/state did not
-- change, regardless of which source(s) observed it — the correct semantics for
-- "did price/listing-visible state change across observations?" and for
-- learning all-source refresh cadence. Per-source participation within that
-- unchanged-state run is preserved via the detail/srp/carousel counts and flags
-- below, which is how SRP/carousel-only refreshes of an unchanged state remain
-- visible to the ML trainer.
--
-- Incremental strategy: affected-listing_id replacement (same pattern as
-- int_listing_state_runs' affected-VIN replacement, Plan 123 Phase 4/final
-- correction), unique_key='listing_id'. listing_id is an ENTITY REPLACEMENT
-- KEY here, not a row-unique key — the output grain is multiple runs per
-- listing_id. delete+insert with this unique_key deletes ALL existing target
-- rows for each affected listing_id and reinserts its complete recomputed run
-- history, so do not add a `unique` data test on listing_id.
--
-- A late-arriving or corrected observation can split a previously single run
-- into two, or collapse what looked like two runs into one — both require
-- recomputing gaps-and-islands over that listing_id's ENTIRE observation
-- history, not just the new rows. On an incremental run, only listing_ids
-- with an observation inside listing_observation_runs_incremental_lookback_days
-- of the target's max(run_ended_at) are treated as affected, but this model
-- then reads ALL of that listing_id's history from
-- int_listing_observation_fingerprints (itself incrementally but fully
-- maintained) to recompute every run.

with affected_listings as (

    select distinct listing_id
    from {{ ref('int_listing_observation_fingerprints') }}

    {% if is_incremental() %}
    where fetched_at >= (
        select coalesce(max(run_ended_at), timestamp '1900-01-01')
               - interval '{{ var("listing_observation_runs_incremental_lookback_days", 3) }}' day
        from {{ this }}
    )
    {% endif %}

),

ordered as (
    select
        f.listing_id,
        f.vin17,
        f.artifact_id,
        f.source,
        f.fetched_at,
        f.price,
        f.mileage,
        f.listing_state,
        md5(concat_ws('|',
            coalesce(cast(f.price as varchar), ''),
            coalesce(cast(f.mileage as varchar), ''),
            coalesce(f.listing_state, '')
        ))                                      as observation_state_key,
        lag(md5(concat_ws('|',
            coalesce(cast(f.price as varchar), ''),
            coalesce(cast(f.mileage as varchar), ''),
            coalesce(f.listing_state, '')
        ))) over (
            partition by f.listing_id order by f.fetched_at, f.artifact_id
        )                                       as prev_state_key
    from {{ ref('int_listing_observation_fingerprints') }} f
    {% if is_incremental() %}
    inner join affected_listings al on al.listing_id = f.listing_id
    {% endif %}
),

flagged as (
    select
        *,
        case
            when prev_state_key is null
              or observation_state_key != prev_state_key
            then 1
            else 0
        end as is_new_run
    from ordered
),

numbered as (
    select
        *,
        sum(is_new_run) over (
            partition by listing_id
            order by fetched_at, artifact_id
            rows between unbounded preceding and current row
        ) as run_id
    from flagged
),

collapsed as (
    select
        listing_id,
        observation_state_key,
        run_id,
        min(fetched_at)                                    as run_started_at,
        max(fetched_at)                                     as run_ended_at,
        count(*)                                            as observation_count,
        count(*) filter (where source = 'detail')           as detail_observation_count,
        count(*) filter (where source = 'srp')              as srp_observation_count,
        count(*) filter (where source = 'carousel')         as carousel_observation_count,
        count(distinct source)                              as distinct_source_count,
        max(vin17)                                          as vin17,
        min(listing_state)                                  as listing_state,
        min(price)                                          as price,
        min(mileage)                                        as mileage
    from numbered
    group by listing_id, observation_state_key, run_id
),

with_lead as (
    select
        *,
        lead(run_started_at) over (
            partition by listing_id order by run_started_at, run_id
        ) as next_observation_started_at
    from collapsed
)

select
    listing_id,
    vin17,
    observation_state_key,
    listing_state,
    price,
    mileage,
    run_started_at,
    run_ended_at,
    observation_count,
    detail_observation_count,
    srp_observation_count,
    carousel_observation_count,
    distinct_source_count,
    detail_observation_count > 0                                        as detail_seen,
    srp_observation_count > 0                                           as srp_seen,
    carousel_observation_count > 0                                      as carousel_seen,
    datediff('hour', run_started_at, run_ended_at)                      as run_duration_hours,
    next_observation_started_at,
    datediff('hour', run_ended_at, next_observation_started_at)         as hours_until_next_observation,
    next_observation_started_at is null                                 as is_open_run
from with_lead
