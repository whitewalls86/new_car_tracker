-- Latest SRP attributes per VIN.
-- Provides make/model/trim/year/msrp and seller info not in mart_vehicle_snapshot.

with ranked as (
    select
        s.vin17 as vin,
        s.make,
        s.model,
        s.vehicle_trim,
        s.model_year,
        s.msrp,
        s.fuel_type,
        s.body_style,
        s.stock_type,
        s.financing_type,
        s.seller_zip,
        s.seller_customer_id,
        s.canonical_detail_url,
        ra.search_key,
        ra.search_scope,
        s.fetched_at,
        s.artifact_id,
        row_number() over (
            partition by s.vin17
            order by s.fetched_at desc, s.artifact_id desc
        ) as rn
    from {{ ref('stg_srp_observations') }} s
    inner join {{ source('public', 'raw_artifacts') }} ra
        on ra.artifact_id = s.artifact_id
    where s.vin17 is not null
)

select
    vin,
    make,
    model,
    vehicle_trim,
    model_year,
    msrp,
    fuel_type,
    body_style,
    stock_type,
    financing_type,
    seller_zip,
    seller_customer_id,
    canonical_detail_url,
    search_key,
    search_scope,
    fetched_at as attributes_observed_at,
    artifact_id as attributes_artifact_id
from ranked
where rn = 1
