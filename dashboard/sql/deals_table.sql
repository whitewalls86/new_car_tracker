-- {filter_clause}: zero or more "AND col IN (?...)" fragments appended by deals.py
SELECT
    make, model, vehicle_trim, model_year, dealer_name,
    current_price, national_median_price, msrp,
    ROUND(msrp_discount_pct::DOUBLE, 1)           AS msrp_off_pct,
    deal_tier,
    ROUND(deal_score::DOUBLE, 1)                  AS deal_score,
    ROUND(national_price_percentile::DOUBLE * 100, 0) AS price_pct,
    days_on_market,
    price_drop_count                              AS drops,
    canonical_detail_url
FROM mart_deal_scores
WHERE 1=1
{filter_clause}
ORDER BY deal_score DESC
