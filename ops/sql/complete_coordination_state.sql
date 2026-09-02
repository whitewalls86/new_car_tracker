-- Release the coordination row once validation evidence passed. Unlike
-- cancel_coordination_state.sql this does NOT bump the generation: the
-- completion receipt is written against the generation that just finished, and
-- bumping here would make the receipt name a generation that never ran.
UPDATE coordination_state
   SET kind = NULL, phase = 'none', targets = '[]'::jsonb,
       scope = '[]'::jsonb, completed_at = now(), updated_at = now()
 WHERE id = 1
RETURNING generation
