-- {filter_clause}: zero or more "AND col IN (?...)" fragments appended by deals.py
SELECT deal_tier, COUNT(*) AS listings
FROM mart_deal_scores
WHERE 1=1
{filter_clause}
GROUP BY deal_tier
ORDER BY CASE deal_tier
    WHEN 'excellent' THEN 1 WHEN 'good' THEN 2
    WHEN 'fair' THEN 3 WHEN 'weak' THEN 4
END
