SELECT vin17, count(*) AS run_count
FROM main.int_listing_state_runs
WHERE vin17 IN (?, ?, ?, ?, ?)
GROUP BY vin17
