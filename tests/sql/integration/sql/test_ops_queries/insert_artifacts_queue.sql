INSERT INTO artifacts_queue (minio_path, artifact_type, fetched_at, status)
               VALUES (%s, 'results_page', now(), 'pending') RETURNING artifact_id
