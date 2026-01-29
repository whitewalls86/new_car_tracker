with candidates as (
    select
        listing_id,
        vin17 as vin,
        fetched_at as vin_observed_at,
        artifact_id as vin_artifact_id,
        row_number() over (
            partition by listing_id
            order by fetched_at desc, artifact_id desc
        ) as rn
    from {{ ref('stg_srp_observations') }}
    where vin17 is not null
)

select
    listing_id,
    vin,
    vin_observed_at,
    vin_artifact_id
from candidates
where rn = 1
