-- {filter_clause}: zero or more "AND col IN (?...)" fragments appended by deals.py
SELECT
    CASE
        WHEN days_on_market <= 7  THEN '0-7 days'
        WHEN days_on_market <= 14 THEN '8-14 days'
        WHEN days_on_market <= 30 THEN '15-30 days'
        WHEN days_on_market <= 60 THEN '31-60 days'
        ELSE '60+ days'
    END AS bucket,
    COUNT(*) AS listings
FROM mart_deal_scores
WHERE 1=1
{filter_clause}
GROUP BY 1
ORDER BY MIN(days_on_market)
