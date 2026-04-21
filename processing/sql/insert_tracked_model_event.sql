-- Record a tracked_model mutation event.
INSERT INTO staging.tracked_model_events
    (search_key, make, model, event_type)
VALUES (%(search_key)s, %(make)s, %(model)s, %(event_type)s)
