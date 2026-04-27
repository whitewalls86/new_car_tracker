-- Rotation schedule: one row per slot. Artifact counts come from ops.artifacts_queue
-- (the Airflow-era source) rather than the legacy runs/scrape_jobs tables.
WITH slot_configs AS (
    SELECT
        rotation_slot,
        string_agg(search_key, ', ' ORDER BY search_key) AS search_keys,
        MAX(last_queued_at) AS last_queued_at
    FROM search_configs
    WHERE enabled = true AND rotation_slot IS NOT NULL
    GROUP BY rotation_slot
),
-- Artifact counts from the most recent scrape for each slot, using artifacts_queue
-- keyed by search_key and fetched within 4 hours of last_queued_at.
slot_artifacts AS (
    SELECT
        sc.rotation_slot,
        COUNT(DISTINCT aq.artifact_id) AS pages,
        COUNT(DISTINCT aq.artifact_id) FILTER (
            WHERE aq.status = 'skip'
        ) AS skipped
    FROM search_configs sc
    JOIN ops.artifacts_queue aq ON aq.search_key = sc.search_key
        AND aq.artifact_type = 'results_page'
        AND aq.fetched_at >= sc.last_queued_at - interval '10 minutes'
        AND aq.fetched_at <= sc.last_queued_at + interval '4 hours'
    WHERE sc.enabled = true AND sc.rotation_slot IS NOT NULL
      AND sc.last_queued_at IS NOT NULL
    GROUP BY sc.rotation_slot
)
SELECT
    c.rotation_slot AS slot,
    c.search_keys,
    c.last_queued_at AT TIME ZONE 'America/Chicago' AS last_fired,
    ROUND(EXTRACT(EPOCH FROM (now() - c.last_queued_at)) / 3600, 1) AS hours_ago,
    COALESCE(sa.pages, 0) AS pages,
    COALESCE(sa.skipped, 0) AS skipped,
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
LEFT JOIN slot_artifacts sa ON sa.rotation_slot = c.rotation_slot
ORDER BY c.rotation_slot
