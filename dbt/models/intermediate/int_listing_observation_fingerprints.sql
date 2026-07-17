{{
  config(
    materialized='incremental',
    unique_key='observation_id',
    incremental_strategy='merge' if target.type == 'spark' else 'delete+insert',
    file_format='iceberg' if target.type == 'spark' else none
  )
}}

-- One row per observed listing per artifact, across detail, SRP, and carousel
-- sources (Plan 123 Phase 2b). int_listing_state_fingerprints is detail-only and
-- keyed on bare artifact_id; that key does not hold here because a single SRP or
-- carousel artifact can carry many listing_ids, so the row key is
-- artifact_id + listing_id instead. vin17 is kept when present/resolved but is
-- NOT required — SRP and carousel rows frequently lack a resolved VIN, and
-- dropping them would defeat the point of an all-source cadence-learning layer.
--
-- parsed_fingerprint hashes every business-state field this project's source
-- rows carry, source-appropriate ones included even when null for another
-- source (e.g. carousel's body/condition, detail/SRP's dealer and seller
-- fields). Carousel dealer/customer fields are inherited context — the
-- carousel's membership is defined by the dealer, so including them is
-- semantically meaningful, not fuzzy.
--
-- Incremental strategy: 'delete+insert' (not 'merge'), matching
-- int_listing_state_fingerprints, for the same portability reason (Plan 118).
--
-- Plan 125 Gate B: 'merge' on observation_id on the spark target (dbt-spark has
-- no delete+insert), equivalent because observation_id is row-unique — see the
-- `unique` test in the schema file, which the row_number() dedupe below
-- guarantees. Iceberg's MERGE enforces that precondition itself: a duplicated
-- observation_id in the source is a cardinality error, not a silent duplicate.
-- The varchar casts in both hashes go through the cast_to_string macro
-- (`cast(x as varchar)` does not parse on Spark; see dbt/macros/dialect.sql).
-- This model's 28-field hash is the widest in the project and is the one the
-- Gate B parity script compares field-for-field on real data.
-- observation_id is the unique_key, so a source row reappearing inside the
-- lookback window replaces its existing target row rather than duplicating it.
-- delete+insert only dedupes against the *existing target* row, so the
-- row_number()-based dedupe below is required to guarantee the unique_key
-- actually holds after every run, exactly as in the detail-only model.
--
-- Dedupe order is fetched_at desc, then written_at desc, then parsed_fingerprint.
-- written_at (silver write time) breaks ties when two rows for the same
-- observation_id share a fetched_at — e.g. a reprocessing correction of an
-- already-fetched artifact, which lands with the same fetched_at as the
-- original but a later written_at. written_at is intentionally excluded from
-- parsed_fingerprint itself: it's processing metadata, not business state.
--
-- On an incremental run, only source rows at or after
-- max(target.fetched_at) minus listing_observation_fingerprint_lookback_days are
-- rescanned, to pick up late-arriving or corrected observations without
-- rescanning the full table. A first run (or --full-refresh) has no target to
-- watermark from, so it scans the full source, matching the non-incremental
-- behavior exactly. Note this window is fetched_at-based: a reprocessing
-- correction whose fetched_at already fell outside the lookback window before
-- the correction landed will not be picked up until the next --full-refresh.

with source_rows as (

    select *
    from {{ ref('stg_observations') }}
    where source in ('detail', 'srp', 'carousel')
      and listing_id is not null

    {% if is_incremental() %}
      and fetched_at >= (
          select coalesce(max(fetched_at), timestamp '1900-01-01')
                 - interval '{{ var("listing_observation_fingerprint_lookback_days", 3) }}' day
          from {{ this }}
      )
    {% endif %}

),

fingerprinted as (

    select
        md5(concat_ws('|',
            {{ cast_to_string('artifact_id') }},
            coalesce(listing_id, '')
        ))                              as observation_id,
        artifact_id,
        listing_id,
        vin17,
        source,
        fetched_at,
        written_at,
        md5(concat_ws('|',
            coalesce(listing_id,                       ''),
            coalesce(vin17,                            ''),
            coalesce(source,                           ''),
            coalesce({{ cast_to_string('price') }},      ''),
            coalesce({{ cast_to_string('mileage') }},    ''),
            coalesce({{ cast_to_string('model_year') }}, ''),
            coalesce(make,                             ''),
            coalesce(model,                            ''),
            coalesce(vehicle_trim,                     ''),
            coalesce(listing_state,                    ''),
            coalesce(canonical_detail_url,              ''),
            coalesce({{ cast_to_string('msrp') }},       ''),
            coalesce(stock_type,                       ''),
            coalesce(fuel_type,                        ''),
            coalesce(body_style,                       ''),
            coalesce(dealer_name,                      ''),
            coalesce({{ cast_to_string('dealer_zip') }}, ''),
            coalesce(dealer_city,                      ''),
            coalesce(dealer_state,                     ''),
            coalesce(customer_id,                      ''),
            coalesce(seller_customer_id,                ''),
            coalesce({{ cast_to_string('seller_zip') }}, ''),
            coalesce(financing_type,                    ''),
            coalesce({{ cast_to_string('page_number') }},      ''),
            coalesce({{ cast_to_string('position_on_page') }}, ''),
            coalesce(trid,                              ''),
            coalesce(isa_context,                       ''),
            coalesce(body,                              ''),
            coalesce(condition,                         '')
        ))                              as parsed_fingerprint,
        price,
        mileage,
        listing_state

    from source_rows

),

-- Plan 125 Gate B (audit F15): ranking split into its own CTE because Spark
-- rejects referencing the parsed_fingerprint alias from the same SELECT list
-- inside a window ORDER BY (UNSUPPORTED_FEATURE.LATERAL_COLUMN_ALIAS_IN_WINDOW)
-- where DuckDB permits it. Same fix and same reasoning as
-- int_listing_state_fingerprints — keeps one copy of the 28-field hash rather
-- than inlining a second one into the ORDER BY.
ranked as (

    select
        *,
        row_number() over (
            partition by artifact_id, listing_id
            order by fetched_at desc, written_at desc, parsed_fingerprint
        )                               as observation_row_number

    from fingerprinted

)

select
    observation_id,
    artifact_id,
    listing_id,
    vin17,
    source,
    fetched_at,
    parsed_fingerprint,
    price,
    mileage,
    listing_state
from ranked
where observation_row_number = 1
