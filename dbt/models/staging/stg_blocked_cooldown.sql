SELECT
  listing_id,
  first_attempt_at,
  last_attempted_at,
  num_of_attempts,
  CASE WHEN num_of_attempts >= 5 THEN NULL
       ELSE last_attempted_at + (interval '1 hour' * (12 * power(2, num_of_attempts::float - 1)))
  END AS next_eligible_at,
  num_of_attempts >= 5 as fully_blocked
FROM {{ source('public', 'blocked_cooldown') }}