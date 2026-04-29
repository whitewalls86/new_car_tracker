-- Hourly scrape volume for the last 14 days, all source types.
-- Dashboard rolls up to daily/weekly as needed.
SELECT
    hour,
    source,
    artifact_count,
    observation_count,
    unique_listings,
    vin_extraction_pct
FROM mart_scrape_volume
WHERE hour >= now() - INTERVAL '14 days'
ORDER BY hour ASC, source
