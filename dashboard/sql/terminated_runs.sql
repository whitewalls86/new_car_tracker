SELECT
    trigger,
    COUNT(*) AS terminated_count,
    MAX(started_at) AT TIME ZONE 'America/Chicago' AS most_recent
FROM runs
WHERE status = 'terminated'
  AND started_at > now() - interval '7 days'
GROUP BY trigger
ORDER BY terminated_count DESC
