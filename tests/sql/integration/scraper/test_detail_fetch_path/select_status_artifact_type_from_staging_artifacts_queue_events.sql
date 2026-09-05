SELECT status, artifact_type, minio_path FROM staging.artifacts_queue_events WHERE artifact_id = %s
