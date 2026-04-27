SELECT
    date_trunc('day', ap.processed_at AT TIME ZONE 'America/Chicago') AS day,
    COUNT(*) AS total_processed,
    COUNT(*) FILTER (
        WHERE ap.meta->>'primary_json_present' = 'true'
    ) AS has_vehicle_data,
    COUNT(*) FILTER (WHERE ap.message LIKE '%403%') AS cloudflare_blocked,
    COUNT(*) FILTER (
        WHERE ap.meta->>'primary_json_present' = 'false'
          AND (ap.message IS NULL OR ap.message NOT ILIKE '%cloudflare%')
    ) AS no_data,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE ap.meta->>'primary_json_present' = 'true')
        / NULLIF(COUNT(*), 0),
        1
    ) AS extraction_pct
FROM artifact_processing ap
WHERE ap.processor LIKE 'cars_detail_page__%'
  AND ap.processed_at > now() - interval '14 days'
GROUP BY 1
ORDER BY 1 DESC
