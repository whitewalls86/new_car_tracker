WITH pending_artifacts AS (
    SELECT COUNT(*) AS number_running,
           MIN(created_at) AS min_started_at
    FROM ops.artifacts_queue
    WHERE status IN ('pending', 'processing')
), running_detail_claims AS (
    SELECT COUNT(*) AS number_running,
           MIN(claimed_at) AS min_started_at
    FROM ops.detail_scrape_claims
    WHERE status = 'running'
)
SELECT di.intent, di.requested_at, di.requested_by,
       pa.number_running + rdc.number_running AS number_running,
       LEAST(pa.min_started_at, rdc.min_started_at) AS min_started_at
FROM deploy_intent di
LEFT JOIN pending_artifacts pa ON 1=1
LEFT JOIN running_detail_claims rdc ON 1=1
WHERE di.id = 1
