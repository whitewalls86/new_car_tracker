SELECT status, artifact_type, minio_path, fetched_at, event_at, listing_id FROM staging.artifacts_queue_events WHERE artifact_id = %s
