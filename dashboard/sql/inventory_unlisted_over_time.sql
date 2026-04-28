SELECT
    date_trunc('day', last_seen_at) AS day,
    COUNT(*) AS vehicles_unlisted
FROM mart_vehicle_snapshot
WHERE listing_state = 'unlisted'
  AND last_seen_at > now() - INTERVAL '30 days'
  AND (make, model) IN (SELECT make, model FROM int_active_make_models)
GROUP BY 1
ORDER BY 1
