SELECT artifact_id, status, run_id, fetched_at, event_at, artifact_type FROM staging.artifacts_queue_events WHERE artifact_id = ANY(%s)
