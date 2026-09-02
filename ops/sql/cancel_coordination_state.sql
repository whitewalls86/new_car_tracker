-- Cancel a request that has not yet been authorized. It bumps the generation,
-- which complete_coordination_state.sql deliberately does not: nothing holding
-- the cancelled generation should be able to act on it afterwards.
UPDATE coordination_state
   SET kind = NULL, phase = 'none', generation = generation + 1,
       targets = '[]'::jsonb,
       scope = '[]'::jsonb, completed_at = now(), updated_at = now()
 WHERE id = 1
RETURNING generation
