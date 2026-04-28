SELECT COUNT(*) AS cnt
FROM mart_deal_scores
WHERE first_seen_at > now() - INTERVAL '24 hours'
