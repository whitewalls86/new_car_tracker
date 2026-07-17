{{
  config(
    materialized='incremental',
    unique_key='scrape_volume_key',
    incremental_strategy='merge' if target.type == 'spark' else 'delete+insert',
    file_format='iceberg' if target.type == 'spark' else none
  )
}}

-- Plan 125 Gate B (F1 canary): 'merge' on the spark target, because dbt-spark
-- has no delete+insert. The audit recommended 'insert_overwrite' here on the
-- grounds that "merge would strand a (hour, source) row that drops out of the
-- recomputed 72h window, whereas delete+insert removes it" -- and called that
-- the one genuine subtlety of the whole strategy port.
--
-- THAT PREMISE IS FALSE, and it was measured, not argued. dbt-duckdb's
-- delete+insert generates:
--     delete from target where (unique_key) in (select (unique_key) from source);
--     insert into target select ... from source;
-- It deletes only keys PRESENT IN THE NEW BATCH. A (hour, source) that
-- disappears from the recomputed window is by definition absent from the
-- source, so it is never deleted -- today's production build strands it too.
-- Verified by replaying that exact SQL: the disappeared key survived.
--
-- So merge is EXACTLY equivalent to current production behaviour, and it is
-- insert_overwrite that would be the behaviour change (it would start removing
-- rows production currently keeps). Fidelity to the DuckDB baseline is what
-- Gate B is measuring, so merge is the correct port. If we later decide the
-- stranded row is a bug worth fixing, that is a deliberate cross-engine
-- modelling change, not a migration detail -- fix it on DuckDB first.

-- Hourly scrape throughput by source type.
-- artifact_count  = distinct artifacts processed in the hour (proxy for scrape batches).
-- observation_count = total parsed rows (one row per listing per artifact).
-- vin_extraction_pct = quality signal for detail scrapes; SRP/carousel is always NULL.
-- One row per (hour, source). Source values: 'srp', 'detail', 'carousel'.
--
-- Incremental (Plan 123 Phase 5): recent-window replacement, same delete+insert
-- base strategy as the other Plan 123 incremental models. scrape_volume_key =
-- md5(concat_ws('|', hour, source)) is a synthetic surrogate for the
-- (hour, source) composite grain — dbt-duckdb's delete+insert unique_key
-- matching works cleanly against a single column. On an incremental run,
-- source_rows is filtered to stg_observations rows with hour at or after
-- max(target.hour) - scrape_volume_incremental_lookback_hours — a contiguous
-- recent-hour window, not a sparse per-row affected-hour lookup — then the
-- whole model rereads and recomputes ALL rows in that window (not just rows
-- newer than the watermark), so a late-arriving row landing partway through
-- an hour still produces a correct full-hour aggregate rather than only
-- counting the new rows. First run and --full-refresh skip the filter and
-- scan the full source, matching prior full-table behavior exactly.

with source_rows as (

    select
        cast(date_trunc('hour', fetched_at) as timestamp) as hour,
        source,
        artifact_id,
        listing_id,
        vin17
    from {{ ref('stg_observations') }}
    where fetched_at is not null

    {% if is_incremental() %}
    and cast(date_trunc('hour', fetched_at) as timestamp) >= (
        select coalesce(max(hour), timestamp '1900-01-01')
               - interval '{{ var("scrape_volume_incremental_lookback_hours", 72) }}' hour
        from {{ this }}
    )
    {% endif %}

)

select
    md5(concat_ws('|', hour, source))                       as scrape_volume_key,
    hour,
    source,
    count(distinct artifact_id)                             as artifact_count,
    count(*)                                                as observation_count,
    count(distinct listing_id)                              as unique_listings,
    count(*) filter (where vin17 is not null)               as valid_vin_count,
    case
        when source = 'detail'
        then round(
            count(*) filter (where vin17 is not null) * 100.0
            / nullif(count(*), 0), 1
        )
    end                                                     as vin_extraction_pct
from source_rows
group by 1, 2, 3
order by hour desc, source
