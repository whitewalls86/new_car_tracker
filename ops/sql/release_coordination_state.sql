-- The target-'none' half of _transition(): clear kind, targets and scope and
-- stamp the phase column belonging to the operation.
--
-- {timestamp_column} is formatted in from ops.routers.coordination._TRANSITIONS,
-- a closed literal table of four column names. It never comes from a request,
-- which is why interpolation is safe here; the phase values are literals in the
-- statement because this half only ever lands on 'none'.
UPDATE coordination_state
   SET kind = NULL, phase = 'none', targets = '[]'::jsonb,
       scope = '[]'::jsonb, {timestamp_column} = now(),
       updated_at = now()
 WHERE id = 1
RETURNING generation
