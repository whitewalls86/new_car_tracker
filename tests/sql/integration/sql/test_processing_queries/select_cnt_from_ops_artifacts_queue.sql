SELECT COUNT(*) AS cnt FROM ops.artifacts_queue WHERE status IN ('pending', 'retry')
