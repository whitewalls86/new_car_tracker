SELECT artifact_id, artifact_type, status, minio_path FROM ops.artifacts_queue WHERE run_id = %s
