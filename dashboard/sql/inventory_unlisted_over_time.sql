SELECT
    date_trunc('day', fetched_at) AS day,
    COUNT(*) AS vehicles_unlisted
FROM int_latest_observation
WHERE listing_state = 'unlisted'
  AND fetched_at > now() - INTERVAL '30 days'
GROUP BY 1
ORDER BY 1
