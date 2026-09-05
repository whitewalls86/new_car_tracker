SELECT event_id, artifact_id, status, event_at,
                      minio_path, artifact_type, fetched_at, listing_id, run_id
               FROM staging.artifacts_queue_events
               WHERE event_id <= (SELECT MAX(event_id) FROM staging.artifacts_queue_events)
