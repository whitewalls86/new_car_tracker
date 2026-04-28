SELECT
    make, model,
    COUNT(*)              AS active_listings,
    ROUND(AVG(current_price)) AS avg_price,
    MIN(current_price)    AS min_price
FROM mart_deal_scores
GROUP BY make, model
ORDER BY active_listings DESC
