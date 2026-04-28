SELECT
    make,
    model,
    total_vins,
    detail_enriched,
    srp_only,
    coverage_pct
FROM mart_inventory_coverage
ORDER BY total_vins DESC
