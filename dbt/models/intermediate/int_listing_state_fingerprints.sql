{{
  config(
    materialized='incremental',
    unique_key='artifact_id',
    incremental_strategy='merge' if target.type == 'spark' else 'delete+insert',
    file_format='iceberg' if target.type == 'spark' else none
  )
}}

-- One row per detail artifact with a valid VIN.
-- parsed_fingerprint hashes all business-state fields. Field inclusion is intentional:
--   listing_id:    included — a relisting (same VIN, new listing_id) is a material state
--                  change and must open a new run without special-case logic
--   listing_state: included — active→unavailable transitions are state changes
--   customer_id:   included — dealer identity is business state; same VIN moved to a
--                  different dealer should open a new run
--   seller_id:     excluded — overlaps with customer_id and is unreliable for detail pages
--   seller_customer_id: excluded — SRP-only UUID field, not present on detail pages
--
-- Incremental strategy: 'delete+insert' (not 'merge') — it's the base dbt-duckdb
-- strategy available regardless of DuckDB version, unlike DuckDB's newer native
-- MERGE. It is also a dbt-postgres strategy.
--
-- NOTE (Plan 125 Gate A): this comment previously claimed delete+insert was
-- "supported by the Postgres/Spark-family adapters this project may migrate onto
-- later (Plan 118)". That is false for dbt-spark, which validates only 'append',
-- 'merge', 'insert_overwrite', and 'microbatch'. This model's migration path is
-- 'merge' on artifact_id — equivalent here because artifact_id is row-unique
-- (see the `unique` test in the schema file). See
-- docs/plan_125_portability_audit.md § "Incremental strategy decision".
--
-- Plan 125 Gate B: that path is now taken — 'merge' on the spark target. The
-- row_number() dedupe below is what makes it safe: Iceberg's MERGE raises a
-- cardinality error if the source carries two rows for one artifact_id, so the
-- dedupe is no longer just a unique_key guarantee, it is a hard precondition.
-- The varchar casts inside the hash go through the cast_to_string macro —
-- `cast(x as varchar)` is a PARSE ERROR on Spark (it demands a length), and
-- since these feed an md5, a wrong spelling would change every fingerprint.
--
-- artifact_id is the unique_key, so a source
-- artifact_id reappearing inside the lookback window deletes and replaces the
-- existing target row rather than duplicating it. delete+insert only dedupes
-- against the *existing target* row, though — it does not collapse multiple
-- rows sharing an artifact_id within the same incremental batch, so the
-- dedupe step below (row_number() = 1) is required to guarantee the
-- unique_key actually holds after every run.
--
-- On an incremental run, only source rows at or after
-- max(target.fetched_at) minus fingerprint_incremental_lookback_days are rescanned,
-- to pick up late-arriving or corrected artifacts without rescanning the full table.
-- A first run (or --full-refresh) has no target to watermark from, so it scans the
-- full source, matching the non-incremental behavior exactly.

with source_rows as (

    select *
    from {{ ref('stg_observations') }}
    where source = 'detail'
      and vin17 is not null

    {% if is_incremental() %}
      and fetched_at >= (
          select coalesce(max(fetched_at), timestamp '1900-01-01')
                 - interval '{{ var("fingerprint_incremental_lookback_days", 3) }}' day
          from {{ this }}
      )
    {% endif %}

),

fingerprinted as (

    select
        vin17,
        listing_id,
        artifact_id,
        fetched_at,
        md5(concat_ws('|',
            coalesce(listing_id,                       ''),
            coalesce(vin17,                            ''),
            coalesce({{ cast_to_string('price') }},      ''),
            coalesce({{ cast_to_string('mileage') }},    ''),
            coalesce({{ cast_to_string('msrp') }},       ''),
            coalesce(make,                             ''),
            coalesce(model,                            ''),
            coalesce(vehicle_trim,                     ''),
            coalesce({{ cast_to_string('model_year') }}, ''),
            coalesce(stock_type,                       ''),
            coalesce(fuel_type,                        ''),
            coalesce(body_style,                       ''),
            coalesce(listing_state,                    ''),
            coalesce(dealer_name,                      ''),
            coalesce(dealer_zip,                       ''),
            coalesce(dealer_city,                      ''),
            coalesce(dealer_state,                     ''),
            coalesce(customer_id,                      '')
        ))                          as parsed_fingerprint,
        price,
        mileage,
        listing_state

    from source_rows

),

-- Plan 125 Gate B (audit F15): the row_number() below used to sit in the same
-- SELECT list as parsed_fingerprint and order by that alias directly. DuckDB
-- allows it (a "lateral column alias"); Spark rejects it outright with
-- UNSUPPORTED_FEATURE.LATERAL_COLUMN_ALIAS_IN_WINDOW. Ranking in its own CTE is
-- portable to both and, unlike inlining the md5 into the ORDER BY, keeps
-- exactly ONE copy of the 18-field hash — two copies that must stay
-- byte-identical forever is the kind of drift this model least tolerates, since
-- the hash IS the row's identity.
ranked as (

    select
        *,
        row_number() over (
            partition by artifact_id
            order by fetched_at desc, parsed_fingerprint
        )                           as artifact_row_number

    from fingerprinted

)

select
    vin17,
    listing_id,
    artifact_id,
    fetched_at,
    parsed_fingerprint,
    price,
    mileage,
    listing_state
from ranked
where artifact_row_number = 1
