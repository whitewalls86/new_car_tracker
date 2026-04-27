SELECT
    processor,
    COUNT(*) FILTER (WHERE status = 'ok') AS ok,
    COUNT(*) FILTER (WHERE status IN ('retry', 'processing')) AS pending,
    COUNT(*) FILTER (
        WHERE status = 'ok' AND message ILIKE '%cloudflare%'
    ) AS cloudflare_blocked,
    COUNT(*) FILTER (
        WHERE status = 'ok' AND meta->>'primary_json_present' = 'true'
    ) AS has_primary_data,
    MAX(processed_at) AT TIME ZONE 'America/Chicago' AS last_processed
FROM artifact_processing
GROUP BY processor
ORDER BY processor
