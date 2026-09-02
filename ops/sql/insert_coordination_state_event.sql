-- Append one coordination transition to the durable event log. It runs on the
-- state mutation's own open transaction, so the event and the state move
-- together or not at all.
INSERT INTO staging.coordination_state_events
    (generation, prior_phase, phase, kind, actor)
VALUES (%s, %s, %s, %s, %s)
