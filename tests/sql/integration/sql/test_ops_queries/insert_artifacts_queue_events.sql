INSERT INTO artifacts_queue_events
                   (artifact_id, status, minio_path, artifact_type, fetched_at)
               VALUES (%s, 'pending', %s, 'results_page', now())
               RETURNING event_id
