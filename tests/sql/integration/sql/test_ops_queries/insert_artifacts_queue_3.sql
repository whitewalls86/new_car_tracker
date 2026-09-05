INSERT INTO artifacts_queue (minio_path, artifact_type, fetched_at, status)
                   VALUES (%s, 'results_page', now(), 'invalid_status')
