-- {filter_clause}: zero or more "AND col IN (?...)" fragments appended by deals.py
SELECT
    model,
    ROUND(AVG(current_price))                     AS avg_price,
    ROUND(AVG(msrp))                              AS avg_msrp,
    ROUND(AVG(msrp_discount_pct)::DOUBLE, 1)      AS avg_msrp_off_pct,
    COUNT(*)                                      AS listings
FROM mart_deal_scores
WHERE msrp IS NOT NULL AND msrp > 0
{filter_clause}
GROUP BY model
ORDER BY avg_msrp_off_pct DESC
