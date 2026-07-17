{{
  config(
    materialized='incremental',
    unique_key='vin',
    incremental_strategy='merge' if target.type == 'spark' else 'delete+insert',
    file_format='iceberg' if target.type == 'spark' else none
  )
}}

-- Price history per VIN aggregated from the price observation event stream.
-- Price drop/increase counts are derived by comparing consecutive prices with LAG().
--
-- Replaces: int_price_events + int_price_history_by_vin + int_latest_price_by_vin
--           + int_listing_days_on_market
--
-- Incremental strategy: affected-VIN replacement (Plan 123 Phase 3), same
-- delete+insert base strategy as int_listing_state_fingerprints (Phase 2).
--
-- NOTE (Plan 125 Gate A): this comment previously claimed the strategy was
-- "portable across the Postgres/Spark-family adapters this project may migrate
-- onto later (Plan 118)". That is false for dbt-spark, which validates only
-- 'append', 'merge', 'insert_overwrite', and 'microbatch'. Migration path here
-- is 'merge' on vin: this model is one row per vin (see the `unique` test in the
-- schema file), so merge replaces that row rather than a multi-row set. The one
-- behavioural gap is that merge cannot remove a vin whose events all disappear;
-- the price event stream is append-only, so that case does not arise. See
-- docs/plan_125_portability_audit.md § "Incremental strategy decision".
--
-- Plan 125 Gate B: that migration path is now taken -- 'merge' on the spark
-- target. merge is safe here only because `vin` is genuinely row-unique, which
-- the schema file's `unique` test enforces on every DuckDB build; Iceberg's
-- MERGE would otherwise fail its cardinality check outright rather than
-- silently duplicate. arg_max/arg_min go through the dialect macros rather than
-- bare max_by/min_by: DuckDB's arg_max ignores rows whose VALUE is null and
-- Spark's max_by does not (measured -- see dbt/macros/dialect.sql).
--
-- Consecutive-price LAG() logic depends on the event
-- immediately before the incremental boundary, so a VIN touched by any new,
-- late, or corrected event inside the lookback window has its ENTIRE price
-- history reread and every aggregate recomputed here — not just the new
-- batch of events.
--
-- days_on_market is intentionally NOT computed in this model anymore. It used
-- to be `datediff('day', min(event_at), now())`, which is time-relative: once
-- this model stopped rebuilding every VIN on every run, an untouched VIN's row
-- would freeze at whatever days_on_market it had at its last recomputation
-- instead of advancing with real time. first_seen_at (stable, event-derived)
-- is kept here; days_on_market is now computed downstream in
-- mart_vehicle_snapshot — a full-table rebuild on every hourly_core run —
-- from first_seen_at against now_ts() at query time, so it stays fresh every
-- run regardless of which VINs this model reprocessed.

with affected_vins as (

    select distinct vin
    from {{ ref('stg_price_events') }}

    {% if is_incremental() %}
    where event_at >= (
        select coalesce(max(last_seen_at), timestamp '1900-01-01')
               - interval '{{ var("price_history_incremental_lookback_days", 3) }}' day
        from {{ this }}
    )
    {% endif %}

),

history as (

    select
        e.vin,
        e.price,
        e.event_at
    from {{ ref('stg_price_events') }} e
    {% if is_incremental() %}
    inner join affected_vins av on av.vin = e.vin
    {% endif %}

),

ordered as (
    select
        vin,
        price,
        event_at,
        lag(price) over (partition by vin order by event_at) as prev_price
    from history
)

select
    vin,
    -- Current price = most recent event
    {{ arg_max('price', 'event_at') }}                                        as current_price,
    max(event_at)                                                             as price_observed_at,
    -- First price = oldest event
    {{ arg_min('price', 'event_at') }}                                        as first_price,
    min(price)                                                                as min_price,
    max(price)                                                                as max_price,
    count(*)                                                                  as total_price_observations,
    count(*) filter (where price < prev_price and prev_price is not null)     as price_drop_count,
    count(*) filter (where price > prev_price and prev_price is not null)     as price_increase_count,
    min(event_at)                                                             as first_seen_at,
    max(event_at)                                                             as last_seen_at
from ordered
group by vin
