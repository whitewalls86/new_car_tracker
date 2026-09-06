INSERT INTO staging.artifacts_queue_events
               (artifact_id, status, event_at, minio_path, 
                artifact_type, fetched_at, listing_id, run_id)
           VALUES (%s, 'pending', %s, %s, 'results_page', %s, 'listing-test', 'run-test')
           RETURNING event_id
