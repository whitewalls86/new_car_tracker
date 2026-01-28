select
  so.listing_id,
  so.vin,
  so.fetched_at as observed_at,
  so.artifact_id,

  ra.filepath,
  ra.url,
  ra.http_status,
  ra.content_type,
  ra.content_bytes,
  ra.sha256,
  ra.error as artifact_error,
  ra.page_num,
  ra.fetched_at as artifact_fetched_at,
  ra.run_id as artifact_run_id,
  ra.source as artifact_source,
  ra.artifact_type as artifact_type,
  ra.search_key,
  ra.search_scope

from {{ ref('stg_srp_observations') }} so
join {{ source('public', 'raw_artifacts') }} ra
  on ra.artifact_id = so.artifact_id
