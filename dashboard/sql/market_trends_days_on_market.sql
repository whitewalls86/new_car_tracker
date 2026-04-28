SELECT
    make, model,
    ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY days_on_market)) AS median_days,
    ROUND(AVG(days_on_market)::DOUBLE, 1)                              AS avg_days,
    MIN(days_on_market)                                                AS min_days,
    MAX(days_on_market)                                                AS max_days,
    COUNT(*)                                                           AS listings
FROM mart_deal_scores
GROUP BY make, model
ORDER BY median_days DESC
