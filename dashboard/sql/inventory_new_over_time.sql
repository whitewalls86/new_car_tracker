SELECT
    date_trunc('day', first_seen_at) AS day,
    make,
    COUNT(*) AS new_listings
FROM mart_deal_scores
WHERE first_seen_at > now() - INTERVAL '30 days'
GROUP BY 1, 2
ORDER BY 1, 2
