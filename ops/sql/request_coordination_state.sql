-- Open one scoped coordination request on the idle singleton row. It bumps the
-- generation and clears every phase timestamp from the previous cycle, so a
-- stale reader holding the old generation cannot match anything.
UPDATE coordination_state
   SET kind = %s, phase = 'requested', generation = generation + 1,
       targets = %s::jsonb,
       scope = %s::jsonb, requested_by = %s, reason = %s,
       requested_at = now(), draining_at = NULL,
       active_at = NULL, validating_at = NULL,
       completed_at = NULL, expected_work = %s::jsonb,
       manifest_location = %s, operator_notes = %s,
       updated_at = now()
 WHERE id = 1
RETURNING generation
