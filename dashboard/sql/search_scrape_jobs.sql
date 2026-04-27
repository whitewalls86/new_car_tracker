-- Recent SRP scrape results by search_key, sourced from ops.artifacts_queue.
-- Groups artifacts fetched within the same ~4-hour window (one rotation fire) per key.
SELECT
    sc.rotation_slot AS slot,
    aq.search_key,
    DATE_TRUNC('hour', aq.fetched_at) AT TIME ZONE 'America/Chicago' AS scrape_hour,
    COUNT(*) AS pages_fetched,
    COUNT(*) FILTER (WHERE aq.status = 'complete') AS pages_complete,
    COUNT(*) FILTER (WHERE aq.status IN ('pending', 'retry')) AS pages_pending,
    COUNT(*) FILTER (WHERE aq.status = 'skip') AS pages_skipped,
    MIN(aq.fetched_at) AT TIME ZONE 'America/Chicago' AS first_artifact,
    MAX(aq.fetched_at) AT TIME ZONE 'America/Chicago' AS last_artifact
FROM ops.artifacts_queue aq
LEFT JOIN search_configs sc ON sc.search_key = aq.search_key
WHERE aq.artifact_type = 'results_page'
  AND aq.fetched_at > now() - interval '7 days'
GROUP BY sc.rotation_slot, aq.search_key, DATE_TRUNC('hour', aq.fetched_at)
ORDER BY scrape_hour DESC, aq.search_key
