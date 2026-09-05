INSERT INTO staging.artifacts_queue_events
    (artifact_id, status, artifact_type, event_at)
VALUES (%s, 'processing', 'detail_page',
        now() - interval '{proc_event_hours_ago} hours')
