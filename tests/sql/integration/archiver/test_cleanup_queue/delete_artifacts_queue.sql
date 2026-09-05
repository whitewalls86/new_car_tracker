DELETE FROM artifacts_queue
               WHERE artifact_id = ANY(%s) AND status IN ('complete', 'skip')
               RETURNING artifact_id
