SELECT
    processor,
    status,
    COUNT(*) AS count,
    MIN(processed_at) AT TIME ZONE 'America/Chicago' AS oldest
FROM artifact_processing
WHERE status IN ('retry', 'processing')
GROUP BY processor, status
ORDER BY count DESC
