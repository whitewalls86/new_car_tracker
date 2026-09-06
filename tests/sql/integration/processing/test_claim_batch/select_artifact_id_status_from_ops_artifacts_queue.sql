SELECT artifact_id, status FROM ops.artifacts_queue WHERE status IN ('pending', 'retry')
