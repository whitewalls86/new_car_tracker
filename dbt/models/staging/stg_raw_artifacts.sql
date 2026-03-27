{{
  config(
    materialized = 'incremental',
    unique_key = 'artifact_id',
    incremental_strategy = 'merge'
  )
}}

SELECT
  artifact_id,
  run_id,
  search_key,
  search_scope,
  fetched_at,
  http_status
FROM {{ source('public', 'raw_artifacts') }}

{% if is_incremental() %}
WHERE artifact_id > (SELECT COALESCE(MAX(artifact_id), 0) FROM {{ this }})
{% endif %}
