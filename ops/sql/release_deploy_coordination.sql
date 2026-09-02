-- Release the coordination row that the legacy deploy facade opened, from
-- whatever phase it reached. Textually the same work as
-- cancel_coordination_state.sql, and kept as its own file on purpose: that one
-- belongs to /coordination/cancel, which refuses anything past 'draining',
-- while this one is the facade's unconditional release of a 'deploy' kind. They
-- are two policies that happen to agree today, not one statement.
UPDATE coordination_state
   SET kind = NULL, phase = 'none',
       generation = generation + 1,
       targets = '[]'::jsonb, scope = '[]'::jsonb,
       completed_at = now(), updated_at = now()
 WHERE id = 1
RETURNING generation
