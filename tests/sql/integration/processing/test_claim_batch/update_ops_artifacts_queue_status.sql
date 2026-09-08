UPDATE ops.artifacts_queue SET status = 'skip' WHERE artifact_id = ANY(%s)
