with srp as (
    select
      s.vin17 as vin,
      s.listing_id,
      s.artifact_id,
      s.fetched_at as observed_at,
      null::text as listing_state,
      s.mileage,
      'srp'::text as source
    from {{ ref('stg_srp_observations') }} s
    where s.vin17 is not null
),

detail as (
    select
        d.vin as vin,
        d.listing_id,
        d.artifact_id,
        d.fetched_at as observed_at,
        d.listing_state,
        d.mileage,
        'detail'::text as source
    from {{ ref('stg_detail_observations') }} d
    where d.vin is not null
),

tier1 as (
    select * from srp
    union all
    select * from detail
),

ranked as (
    select
        *,
        row_number() over (
            partition by vin
            order by observed_at desc, artifact_id desc
        ) as rn
    from tier1
)

select
    vin,
    listing_id,
    artifact_id,
    observed_at,
    listing_state,
    mileage,
    source
from ranked
where rn = 1
