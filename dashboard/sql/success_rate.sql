SELECT
    date_trunc('day', fetched_at AT TIME ZONE 'America/Chicago') AS day,
    CASE
        WHEN http_status = 200 THEN '200 OK'
        WHEN http_status = 403 THEN '403 Blocked'
        WHEN http_status IS NULL THEN 'Error/Timeout'
        ELSE http_status::text
    END AS result,
    COUNT(*) AS fetches
FROM raw_artifacts
WHERE artifact_type = '{artifact_type}'
  AND fetched_at > now() - interval '{interval}'
GROUP BY 1, 2
ORDER BY 1, 2
