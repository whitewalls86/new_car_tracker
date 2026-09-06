INSERT INTO staging.artifacts_queue_events
    (artifact_id, status, artifact_type, event_at)
VALUES (%s, 'processing', 'detail_page', now() - (%s || ' hours')::interval)
