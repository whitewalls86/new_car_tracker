SELECT
    pid,
    state,
    ROUND(EXTRACT(EPOCH FROM (now() - query_start))::numeric, 1) AS duration_s,
    LEFT(query, 80) AS query
FROM pg_stat_activity
WHERE state = 'active'
  AND query_start < now() - interval '5 seconds'
  AND backend_type = 'client backend'
ORDER BY duration_s DESC
