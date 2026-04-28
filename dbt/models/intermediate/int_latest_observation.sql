{{
  config(materialized='table')
}}

-- One row per VIN: the most recent observation, preferring detail > srp > carousel.
-- This is the canonical source for vehicle attributes (make/model/trim/year/dealer etc.)
-- and current listing state.
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
)
where _rn = 1
