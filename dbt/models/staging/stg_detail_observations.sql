select
  d.id,
  d.artifact_id,
  d.fetched_at,
  d.listing_id,
  d.vin,
  case
      when d.vin is not null and length(d.vin) = 17 and upper(d.vin) ~ '^[A-Z0-9]{17}$' then upper(d.vin)
      else null
  end as vin17,
  d.listing_state,
  d.price,
  d.mileage,
  d.msrp,
  d.stock_type,
  d.dealer_name,
  d.dealer_zip,
  d.customer_id,

  -- URL we actually fetched for this detail artifact
  ra.url as canonical_detail_url

from {{ source('public', 'detail_observations') }} d
left join {{ source('public', 'raw_artifacts') }} ra
  on ra.artifact_id = d.artifact_id
