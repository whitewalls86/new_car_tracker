SELECT
  customer_id,
  name,
  city,
  state,
  zip,
  phone,
  rating
FROM {{ source('public', 'dealers') }}
WHERE LENGTH(customer_id) < 36