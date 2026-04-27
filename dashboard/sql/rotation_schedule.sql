WITH slot_configs AS (
    SELECT
        rotation_slot,
        string_agg(search_key, ', ' ORDER BY search_key) AS search_keys,
        MAX(last_queued_at) AS last_queued_at
    FROM search_configs
    WHERE enabled = true AND rotation_slot IS NOT NULL
    GROUP BY rotation_slot
),
slot_last_run AS (
    SELECT DISTINCT ON (sc.rotation_slot)
        sc.rotation_slot,
        r.run_id,
        r.status AS run_status,
        r.started_at
    FROM search_configs sc
    JOIN scrape_jobs j ON j.search_key = sc.search_key
    JOIN runs r ON r.run_id = j.run_id AND r.trigger = 'search scrape'
    WHERE sc.enabled = true AND sc.rotation_slot IS NOT NULL
    ORDER BY sc.rotation_slot, r.started_at DESC
),
slot_results AS (
    SELECT
        slr.rotation_slot,
        COUNT(DISTINCT a.artifact_id) AS pages,
        COUNT(DISTINCT a.artifact_id) FILTER (
            WHERE a.http_status IS NULL OR a.http_status >= 400
        ) AS errors,
        COUNT(DISTINCT so.vin) AS vins_observed
    FROM slot_last_run slr
    JOIN scrape_jobs j ON j.run_id = slr.run_id
        AND j.search_key IN (
            SELECT search_key FROM search_configs
            WHERE rotation_slot = slr.rotation_slot
        )
    JOIN raw_artifacts a ON a.run_id = slr.run_id
        AND a.artifact_type = 'results_page'
        AND a.search_key = j.search_key
        AND a.search_scope = j.scope
    LEFT JOIN srp_observations so ON so.artifact_id = a.artifact_id
        AND so.vin IS NOT NULL
    GROUP BY slr.rotation_slot
)
SELECT
    c.rotation_slot AS slot,
    c.search_keys,
    c.last_queued_at AT TIME ZONE 'America/Chicago' AS last_fired,
    ROUND(EXTRACT(EPOCH FROM (now() - c.last_queued_at)) / 3600, 1) AS hours_ago,
    COALESCE(slr.run_status, '-') AS last_status,
    COALESCE(res.pages, 0) AS pages,
    COALESCE(res.errors, 0) AS errors,
    COALESCE(res.vins_observed, 0) AS vins_observed,
    (c.last_queued_at + interval '1439 minutes')
        AT TIME ZONE 'America/Chicago' AS next_eligible,
    CASE
        WHEN c.last_queued_at IS NULL THEN 'Ready now'
        WHEN now() > c.last_queued_at + interval '1439 minutes' THEN 'Ready now'
        ELSE 'In ' || ROUND(EXTRACT(EPOCH FROM (
            c.last_queued_at + interval '1439 minutes' - now()
        )) / 3600, 1)::text || 'h'
    END AS next_status
FROM slot_configs c
LEFT JOIN slot_last_run slr ON slr.rotation_slot = c.rotation_slot
LEFT JOIN slot_results res ON res.rotation_slot = c.rotation_slot
ORDER BY c.rotation_slot
