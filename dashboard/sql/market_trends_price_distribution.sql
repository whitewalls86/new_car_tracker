SELECT
    make, model,
    ROUND(percentile_cont(0.10) WITHIN GROUP (ORDER BY current_price)) AS p10,
    ROUND(percentile_cont(0.25) WITHIN GROUP (ORDER BY current_price)) AS p25,
    ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY current_price)) AS median,
    ROUND(percentile_cont(0.75) WITHIN GROUP (ORDER BY current_price)) AS p75,
    ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY current_price)) AS p90,
    COUNT(*)                                                           AS listings
FROM mart_deal_scores
GROUP BY make, model
ORDER BY median DESC
