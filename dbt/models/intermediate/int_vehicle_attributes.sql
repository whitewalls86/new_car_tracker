-- Latest vehicle attributes per VIN from SRP and detail observations.
-- Freshest observation wins regardless of source.

with srp_attrs as (
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
        'srp' as attributes_source
    from {{ ref('stg_srp_observations') }} s
    inner join {{ source('public', 'raw_artifacts') }} ra
        on ra.artifact_id = s.artifact_id
    where s.vin17 is not null
),

detail_attrs as (
    select
        d.vin17 as vin,
        d.make,
        d.model,
        d.vehicle_trim,
        d.model_year,
        d.msrp,
        d.fuel_type,
        d.body_style,
        d.stock_type,
        null::text as financing_type,
        d.dealer_zip as seller_zip,
        d.customer_id as seller_customer_id,
        d.canonical_detail_url,
        null::text as search_key,
        null::text as search_scope,
        d.fetched_at,
        d.artifact_id,
        'detail' as attributes_source
    from {{ ref('stg_detail_observations') }} d
    where d.vin17 is not null
      and d.make is not null
),

combined as (
    select * from srp_attrs
    union all
    select * from detail_attrs
),

ranked as (
    select
        *,
        row_number() over (
            partition by vin
            order by fetched_at desc, artifact_id desc
        ) as rn
    from combined
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
    artifact_id as attributes_artifact_id,
    attributes_source
from ranked
where rn = 1
