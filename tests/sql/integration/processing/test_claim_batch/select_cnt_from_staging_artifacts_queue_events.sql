SELECT COUNT(*) AS cnt FROM staging.artifacts_queue_events WHERE artifact_id = %s AND status = 'processing'
