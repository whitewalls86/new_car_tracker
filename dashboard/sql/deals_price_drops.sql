-- {filter_clause}: zero or more "AND col IN (?...)" fragments appended by deals.py
SELECT
    make, model, vehicle_trim, model_year, dealer_name,
    current_price, first_price,
    current_price - first_price               AS price_change,
    ROUND(total_price_drop_pct::DOUBLE, 1)    AS total_drop_pct,
    price_drop_count                          AS drops,
    days_on_market,
    canonical_detail_url
FROM mart_deal_scores
WHERE price_drop_count > 0
{filter_clause}
ORDER BY total_price_drop_pct DESC
