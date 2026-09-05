SELECT artifact_type, status, listing_id, minio_path FROM ops.artifacts_queue WHERE artifact_id = %s
