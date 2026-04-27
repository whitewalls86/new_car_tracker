SELECT
    COUNT(*) FILTER (WHERE state = 'active') AS active,
    COUNT(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_tx,
    ROUND(MAX(
        CASE
            WHEN state = 'active' AND query_start IS NOT NULL
            THEN EXTRACT(EPOCH FROM (now() - query_start))
        END
    )::numeric, 1) AS longest_query_s
FROM pg_stat_activity
WHERE backend_type = 'client backend'
