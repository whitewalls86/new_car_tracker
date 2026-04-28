SELECT
    make, model,
    COUNT(*)                                   AS tracked_listings,
    ROUND(AVG(national_listing_count))         AS avg_national_supply,
    ROUND(AVG(current_price))                  AS avg_price,
    ROUND(AVG(msrp_discount_pct)::DOUBLE, 1)   AS avg_msrp_off_pct
FROM mart_deal_scores
WHERE (make, model) IN (SELECT make, model FROM int_active_make_models)
GROUP BY make, model
ORDER BY tracked_listings DESC
