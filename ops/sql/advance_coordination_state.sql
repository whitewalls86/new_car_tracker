-- The ordinary half of _transition(): move to the next phase and stamp its
-- column. {timestamp_column} is formatted in from
-- ops.routers.coordination._TRANSITIONS, a closed literal table of four column
-- names; the phase itself is a bound parameter, not interpolated.
UPDATE coordination_state
   SET phase = %s, {timestamp_column} = now(), updated_at = now()
 WHERE id = 1
RETURNING generation
