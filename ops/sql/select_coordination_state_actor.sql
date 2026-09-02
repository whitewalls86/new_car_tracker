-- The locked confirming read taken before a state transition: phase and
-- generation decide whether the move is legal, and requested_by is the actor
-- the resulting transition event is attributed to.
SELECT phase, generation, kind, requested_by
  FROM coordination_state WHERE id = 1
