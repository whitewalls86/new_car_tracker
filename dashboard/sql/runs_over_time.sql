SELECT
    date_trunc('day', started_at AT TIME ZONE 'America/Chicago') AS day,
    trigger,
    COUNT(*) AS runs,
    COUNT(*) FILTER (WHERE status = 'success') AS successful,
    COUNT(*) FILTER (WHERE status = 'terminated') AS terminated,
    COUNT(*) FILTER (WHERE status = 'failed') AS failed
FROM runs
WHERE started_at > now() - interval '7 days'
  AND status NOT IN ('skipped', 'terminated')
GROUP BY 1, 2
ORDER BY 1, 2
