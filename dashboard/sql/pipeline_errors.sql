SELECT
    occurred_at AT TIME ZONE 'America/Chicago' AS occurred_at_ct,
    workflow_name,
    node_name,
    error_type,
    error_message
FROM pipeline_errors
ORDER BY occurred_at DESC
LIMIT 50
