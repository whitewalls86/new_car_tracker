{{
  config(materialized='table')
}}

-- One row per VIN: the best data-rich observation, preferring detail > srp > carousel.
-- Requires make IS NOT NULL so vehicle attributes (make/model/trim/year/dealer etc.)
-- are always usable downstream.  Unlisted detail pages return no vehicle JSON → NULL make;
-- those rows are intentionally excluded here.  listing_state for those VINs is handled
-- via the latest_state CTE in mart_vehicle_snapshot, which reads stg_observations directly.
--
-- Replaces: int_vehicle_attributes + int_latest_tier1_observation_by_vin + int_listing_to_vin

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
    from {{ ref('stg_observations') }}
    where vin17 is not null
      and make is not null
)
where _rn = 1
