INSERT INTO artifacts_queue_events
                   (artifact_id, status, minio_path, artifact_type)
               VALUES (%s, 'pending', %s, 'results_page')
               RETURNING event_at
