DELETE FROM staging.artifacts_queue_events WHERE artifact_id = ANY(%s)
