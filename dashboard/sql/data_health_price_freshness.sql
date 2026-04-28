SELECT
    make,
    model,
    total_vins,
    fresh_lt_1d,
    fresh_1_3d,
    fresh_4_7d,
    fresh_8_14d,
    stale_gt_14d,
    fresh_lt_7d_pct
FROM mart_price_freshness_trend
ORDER BY stale_gt_14d DESC, total_vins DESC
