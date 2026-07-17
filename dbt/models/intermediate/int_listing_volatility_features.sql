{{
  config(
    materialized='table',
    file_format='iceberg' if target.type == 'spark' else none
  )
}}

-- One row per VIN — current-state feature row for Plan 112 backtesting.
-- Grain is the open run per VIN (is_open_run = true in int_listing_state_runs).
-- Dealer and make/model run-length rates are computed from completed runs only
-- (open run excluded) to avoid in-progress duration distorting the baseline.
--
-- Pass --vars '{"as_of_at": "2026-06-01T00:00:00+00:00"}' for reproducible backtests.
-- Defaults to now() when as_of_at is unset.
--
-- Backtest isolation boundary: inline sources (stg_observations, stg_price_events)
-- are all filtered to fetched_at/event_at <= as_of_at to prevent future data leaking
-- into price windows, SRP recency, and metadata resolution.
-- Pre-materialized joins (int_listing_state_runs, int_price_history, int_benchmarks,
-- int_listing_observation_runs) are NOT filtered here; Plan 112 must snapshot those
-- tables at the as_of point.
--
-- Plan 123 final modeling correction: all_source_* columns below join the
-- listing's current OPEN run from int_listing_observation_runs (all-source:
-- detail, SRP, carousel cadence), separate from the detail-only state-run
-- columns above. This does not replace the detail-only semantics — it makes
-- SRP/carousel refresh cadence visible to the ML trainer alongside them.
-- Joined by listing_id (not vin17) since int_listing_observation_runs is
-- listing_id-grained. A listing with no all-source observation run (should
-- not happen in practice, since detail observations feed both models) yields
-- NULL/0 defaults rather than dropping the row.

-- Plan 125 Gate B: the capstone of the ported chain, and the densest
-- concentration of dialect items in the project — arg_max x3, median x2,
-- datediff('day') x4, the as_of_at timestamptz cast, and a bare ::numeric.
-- All go through dbt/macros/dialect.sql; see that file for why the obvious
-- Spark spelling is wrong for most of them.
--
-- The as_of_at cast is the subtlest: Spark has no TIMESTAMPTZ at all. Its
-- TIMESTAMP is instant-typed and resolves this literal's offset against
-- spark.sql.session.timeZone, so cast_to_timestamptz is only equivalent
-- because spark_conf_for_dbt_session() pins that to UTC. Without the pin every
-- backtest as_of boundary would silently shift by the host's offset — which
-- would corrupt backtest results rather than fail.

with as_of as (
    select
        {% if var('as_of_at', '') %}
            {{ cast_to_timestamptz("'" ~ var("as_of_at") ~ "'") }}
        {% else %}
            now()
        {% endif %} as ts
),

vin_listing_meta as (
    -- Most-recent metadata per (vin17, listing_id) as of as_of_at.
    -- Upper-bound filter prevents future observations from shifting arg_max.
    select
        o.vin17,
        o.listing_id,
        {{ arg_max('o.customer_id', 'o.fetched_at') }} as customer_id,
        {{ arg_max('o.make',        'o.fetched_at') }} as make,
        {{ arg_max('o.model',       'o.fetched_at') }} as model
    from {{ ref('stg_observations') }} o
    cross join as_of a
    where o.source = 'detail'
      and o.vin17 is not null
      and o.fetched_at <= a.ts
    group by o.vin17, o.listing_id
),

runs_with_meta as (
    select
        r.vin17,
        r.listing_id,
        r.parsed_fingerprint,
        r.listing_state,
        r.run_started_at,
        r.run_ended_at,
        r.artifact_count,
        r.run_duration_hours,
        r.is_open_run,
        m.customer_id,
        m.make,
        m.model
    from {{ ref('int_listing_state_runs') }} r
    left join vin_listing_meta m using (vin17, listing_id)
),

open_runs as (
    select *
    from runs_with_meta
    where is_open_run = true
),

vin_stats as (
    select
        vin17,
        count(*) - 1               as total_state_changes,
        count(distinct listing_id) as listing_id_change_count,
        min(run_started_at)        as first_seen_at,
        max(run_ended_at)          as latest_fetched_at
    from runs_with_meta
    group by vin17
),

dealer_stats as (
    select
        customer_id,
        avg(run_duration_hours)                       as dealer_avg_run_length_hours,
        {{ median_of('run_duration_hours') }}         as dealer_median_run_length_hours
    from runs_with_meta
    where not is_open_run
      and customer_id is not null
    group by customer_id
),

make_model_stats as (
    select
        make,
        model,
        avg(run_duration_hours)                       as make_model_avg_run_length_hours,
        {{ median_of('run_duration_hours') }}         as make_model_median_run_length_hours
    from runs_with_meta
    where not is_open_run
      and make is not null
      and model is not null
    group by make, model
),

-- listing_state is now carried directly in int_listing_state_runs, so we
-- can compute transitions from runs_with_meta without a range-join back to fingerprints.
run_state_transitions as (
    select
        vin17,
        listing_state                                                    as run_listing_state,
        lag(listing_state) over (
            partition by vin17 order by run_started_at, parsed_fingerprint
        )                                                                as prev_listing_state
    from runs_with_meta
),

listing_state_change_counts as (
    select
        vin17,
        count(*) filter (
            where run_listing_state != prev_listing_state
              and prev_listing_state is not null
        ) as listing_state_change_count
    from run_state_transitions
    group by vin17
),

-- Filter to <= as_of_at BEFORE computing lag so future events cannot shift
-- prev_price for events near the as_of boundary.
price_events_with_lag as (
    select
        p.vin,
        p.price,
        p.event_at,
        lag(p.price) over (partition by p.vin order by p.event_at) as prev_price,
        a.ts as as_of_ts
    from {{ ref('stg_price_events') }} p
    cross join as_of a
    where p.event_at <= a.ts
),

price_changes as (
    select
        vin,
        count(*) filter (
            where event_at >= as_of_ts - interval '7 days'
              and prev_price is not null
              and price != prev_price
        ) as price_change_count_7d,
        count(*) filter (
            where event_at >= as_of_ts - interval '30 days'
              and prev_price is not null
              and price != prev_price
        ) as price_change_count_30d
    from price_events_with_lag
    group by vin
),

srp_latest as (
    select
        o.listing_id,
        max(o.fetched_at) as recent_srp_seen_at
    from {{ ref('stg_observations') }} o
    cross join as_of a
    where o.source = 'srp'
      and o.fetched_at <= a.ts
    group by o.listing_id
),

open_observation_runs as (
    select *
    from {{ ref('int_listing_observation_runs') }}
    where is_open_run = true
)

select
    -- Identity
    o.vin17,
    o.listing_id,
    vs.latest_fetched_at,
    vs.first_seen_at,

    -- State history
    vs.total_state_changes,
    vs.listing_id_change_count,
    {{ datediff_days('o.run_started_at', 'ao.ts') }} as days_since_last_state_change,
    o.artifact_count                                as unchanged_observation_streak,
    coalesce(lsc.listing_state_change_count, 0)     as listing_state_change_count,

    -- Price signals
    ph.current_price,
    coalesce(pc.price_change_count_7d,  0)          as price_change_count_7d,
    coalesce(pc.price_change_count_30d, 0)          as price_change_count_30d,
    -- The one place a bare ::numeric is exposed RAW, with no final rounding to
    -- hide the difference: DuckDB's division promotes this to DOUBLE
    -- (0.9602222222222222), while Spark's bare `decimal` would yield
    -- decimal(21,11) (0.96022222222) -- a different value and a different
    -- column type. cast_to_numeric reproduces the DOUBLE. Measured, Gate B.
    case
        when bm.national_median_price > 0
        then {{ cast_to_numeric('ph.current_price') }} / bm.national_median_price
        else null
    end                                             as price_vs_make_model_median,

    -- Market / DOM signals
    -- Computed from first_seen_at to as_of_at rather than wall-clock now()
    -- (int_price_history no longer exposes a days_on_market column at all,
    -- as of Plan 123 Phase 3 — see mart_vehicle_snapshot for the hourly,
    -- now()-based equivalent), so this stays reproducible for backtests.
    {{ datediff_days('vs.first_seen_at', 'ao.ts') }} as listing_days_on_market,
    ds.dealer_avg_run_length_hours,
    ds.dealer_median_run_length_hours,
    mms.make_model_avg_run_length_hours,
    mms.make_model_median_run_length_hours,

    -- Pipeline signals
    sl.recent_srp_seen_at,
    {{ datediff_days('sl.recent_srp_seen_at', 'ao.ts') }} as days_since_srp_seen,

    -- All-source observation cadence (Plan 123 final modeling correction)
    oor.run_started_at                                     as all_source_run_started_at,
    case
        when oor.run_started_at is not null
        then {{ datediff_days('oor.run_started_at', 'ao.ts') }}
        else null
    end                                                     as days_since_last_all_source_change,
    coalesce(oor.observation_count, 0)                      as all_source_unchanged_observation_streak,
    coalesce(oor.detail_observation_count, 0)               as all_source_detail_observation_count,
    coalesce(oor.srp_observation_count, 0)                  as all_source_srp_observation_count,
    coalesce(oor.carousel_observation_count, 0)             as all_source_carousel_observation_count,
    coalesce(oor.srp_seen, false) or coalesce(oor.carousel_seen, false)
                                                             as all_source_non_detail_refresh_seen

from open_runs o
cross join as_of ao
join vin_stats vs                             using (vin17)
left join listing_state_change_counts lsc     using (vin17)
left join price_changes pc                    on pc.vin  = o.vin17
left join {{ ref('int_price_history') }} ph   on ph.vin  = o.vin17
left join {{ ref('int_benchmarks') }} bm      on bm.make = o.make and bm.model = o.model
left join dealer_stats ds                     on ds.customer_id = o.customer_id
left join make_model_stats mms                on mms.make = o.make and mms.model = o.model
left join srp_latest sl                       on sl.listing_id = o.listing_id
left join open_observation_runs oor           on oor.listing_id = o.listing_id
