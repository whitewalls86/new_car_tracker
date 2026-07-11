{{
  config(
    materialized='incremental',
    unique_key='vin17',
    incremental_strategy='delete+insert'
  )
}}

-- One row per VIN: the best data-rich observation, preferring detail > srp > carousel.
-- Requires make IS NOT NULL so vehicle attributes (make/model/trim/year/dealer etc.)
-- are always usable downstream.  Unlisted detail pages return no vehicle JSON → NULL make;
-- those rows are intentionally excluded here.  listing_state for those VINs is handled
-- via the latest_state CTE in mart_vehicle_snapshot, which reads stg_observations directly.
--
-- Replaces: int_vehicle_attributes + int_latest_tier1_observation_by_vin + int_listing_to_vin
--
-- Incremental strategy: affected-VIN replacement (Plan 123 Phase 5), same
-- delete+insert base strategy as int_price_history (Phase 3) and
-- int_listing_state_runs (Phase 4). This is NOT a simple "latest by time"
-- model: source-priority ranking (detail > srp > carousel, checked before
-- recency) means the winning row for a VIN can be an OLDER detail
-- observation that beats a newer SRP/carousel one. So a VIN touched by any
-- new, late, or corrected observation inside the lookback window has its
-- ENTIRE observation history reread and reranked here — not just the recent
-- rows — or an older detail row could be incorrectly dropped in favor of a
-- newer but lower-priority observation. On an incremental run, a vin17 is
-- "affected" if it has a stg_observations row (any source, any make) with
-- fetched_at at or after max(target.fetched_at) minus
-- latest_observation_incremental_lookback_days; that recent lookback is used
-- only to discover which VINs changed, never to filter which rows compete
-- for the winning rank. First run and --full-refresh skip the filter and
-- rank the full source, matching prior full-table behavior exactly.

with affected_vins as (

    select distinct vin17
    from {{ ref('stg_observations') }}
    where vin17 is not null

    {% if is_incremental() %}
    and fetched_at >= (
        select coalesce(max(fetched_at), timestamp '1900-01-01')
               - interval '{{ var("latest_observation_incremental_lookback_days", 3) }}' day
        from {{ this }}
    )
    {% endif %}

),

candidates as (

    select o.*
    from {{ ref('stg_observations') }} o
    {% if is_incremental() %}
    inner join affected_vins av on av.vin17 = o.vin17
    {% endif %}
    where o.vin17 is not null
      and o.make is not null

)

select * exclude (_rn)
from (
    select
        *,
        row_number() over (
            partition by vin17
            order by
                case source when 'detail' then 1 when 'srp' then 2 else 3 end,
                fetched_at desc,
                artifact_id desc
        ) as _rn
    from candidates
)
where _rn = 1
