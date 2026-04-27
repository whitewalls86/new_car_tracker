SELECT
    date_trunc('hour', processed_at AT TIME ZONE 'America/Chicago') AS hour,
    processor,
    COUNT(*) AS processed,
    COUNT(*) FILTER (WHERE status = 'ok') AS ok,
    COUNT(*) FILTER (WHERE status NOT IN ('ok')) AS errors
FROM artifact_processing
WHERE processed_at > now() - interval '24 hours'
GROUP BY 1, 2
ORDER BY 1 DESC, 2
