{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'merge'
  )
}}

select
  id,
  artifact_id,
  fetched_at,
  source_listing_id,
  listing_id,
  price,
  mileage,
  body,
  condition,
  year
from {{ source('public', 'detail_carousel_hints') }}

{% if is_incremental() %}
WHERE id > (SELECT COALESCE(MAX(id), 0) FROM {{ this }})
{% endif %}
