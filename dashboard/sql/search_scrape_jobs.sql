SELECT
    r.run_id,
    r.started_at AT TIME ZONE 'America/Chicago' AS run_started,
    r.status AS run_status,
    j.search_key,
    j.scope,
    j.status AS job_status,
    j.artifact_count,
    COUNT(srp.vin) AS vins_recorded,
    COUNT(srp.vin) FILTER (WHERE pe.vin IS NULL) AS new_vins_recorded
FROM runs r
JOIN scrape_jobs j ON j.run_id = r.run_id
LEFT JOIN raw_artifacts ra
    ON j.scope = ra.search_scope
    AND ra.run_id = r.run_id
    AND ra.search_key = j.search_key
LEFT JOIN analytics.stg_srp_observations srp ON ra.artifact_id = srp.artifact_id
LEFT JOIN (
    SELECT vin, MIN(observed_at) AS first_seen
    FROM analytics.int_price_events
    GROUP BY vin
) pe ON srp.vin17 = pe.vin AND pe.first_seen < r.started_at
WHERE r.trigger = 'search scrape'
  AND r.started_at > now() - interval '7 days'
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY r.started_at DESC, j.search_key, j.scope
