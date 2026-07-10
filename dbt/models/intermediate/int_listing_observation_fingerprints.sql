{{
  config(
    materialized='incremental',
    unique_key='observation_id',
    incremental_strategy='delete+insert'
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
-- observation_id is the unique_key, so a source row reappearing inside the
-- lookback window replaces its existing target row rather than duplicating it.
-- delete+insert only dedupes against the *existing target* row, so the
-- row_number()-based dedupe below is required to guarantee the unique_key
-- actually holds after every run, exactly as in the detail-only model.
--
-- On an incremental run, only source rows at or after
-- max(target.fetched_at) minus listing_observation_fingerprint_lookback_days are
-- rescanned, to pick up late-arriving or corrected observations without
-- rescanning the full table. A first run (or --full-refresh) has no target to
-- watermark from, so it scans the full source, matching the non-incremental
-- behavior exactly.

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
            cast(artifact_id as varchar),
            coalesce(listing_id, '')
        ))                              as observation_id,
        artifact_id,
        listing_id,
        vin17,
        source,
        fetched_at,
        md5(concat_ws('|',
            coalesce(listing_id,                       ''),
            coalesce(vin17,                            ''),
            coalesce(source,                           ''),
            coalesce(cast(price       as varchar),     ''),
            coalesce(cast(mileage     as varchar),     ''),
            coalesce(cast(model_year  as varchar),     ''),
            coalesce(make,                             ''),
            coalesce(model,                            ''),
            coalesce(vehicle_trim,                     ''),
            coalesce(listing_state,                    ''),
            coalesce(canonical_detail_url,              ''),
            coalesce(cast(msrp        as varchar),     ''),
            coalesce(stock_type,                       ''),
            coalesce(fuel_type,                        ''),
            coalesce(body_style,                       ''),
            coalesce(dealer_name,                      ''),
            coalesce(cast(dealer_zip as varchar),      ''),
            coalesce(dealer_city,                      ''),
            coalesce(dealer_state,                     ''),
            coalesce(customer_id,                      ''),
            coalesce(seller_customer_id,                ''),
            coalesce(cast(seller_zip as varchar),      ''),
            coalesce(financing_type,                    ''),
            coalesce(cast(page_number as varchar),      ''),
            coalesce(cast(position_on_page as varchar), ''),
            coalesce(trid,                              ''),
            coalesce(isa_context,                       ''),
            coalesce(body,                              ''),
            coalesce(condition,                         '')
        ))                              as parsed_fingerprint,
        price,
        mileage,
        listing_state,
        row_number() over (
            partition by artifact_id, listing_id
            order by fetched_at desc, parsed_fingerprint
        )                               as observation_row_number

    from source_rows

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
from fingerprinted
where observation_row_number = 1
