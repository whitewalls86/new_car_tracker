SELECT
    COALESCE(dealer_name, customer_id) AS dealer,
    make, model,
    COUNT(*)              AS active_listings,
    ROUND(AVG(current_price)) AS avg_price,
    MIN(current_price)    AS min_price
FROM mart_deal_scores
WHERE customer_id IS NOT NULL
GROUP BY COALESCE(dealer_name, customer_id), make, model
ORDER BY active_listings DESC
LIMIT 50
