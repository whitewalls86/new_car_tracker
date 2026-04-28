{{
  config(materialized='view')
}}

-- Active search configurations from Postgres (via postgres_scan).
-- Provides the set of make/model combinations we are actively tracking.

select
    search_key,
    enabled,
    source,
    rotation_slot,
    last_queued_at,
    params->>'zip'                                     as zip,
    (params->>'radius_miles')::int                     as radius_miles,
    (params->>'max_listings')::int                     as max_listings,
    (params->>'max_safety_pages')::int                 as max_safety_pages,
    json_extract_string(params, '$.makes[0]')          as make_slug,
    json_extract_string(params, '$.models[0]')         as model_slug
from {{ source('public', 'search_configs') }}
