SELECT
  search_key,
  enabled,
  source,
  rotation_slot,
  last_queued_at,
  params->>'zip'                       AS zip,
  (params->>'radius_miles')::int       AS radius_miles,
  (params->>'max_listings')::int       AS max_listings,
  (params->>'max_safety_pages')::int   AS max_safety_pages,
  params->'makes'->>0                  AS make_slug,
  params->'models'->>0                 AS model_slug
FROM {{ source('public', 'search_configs') }}
