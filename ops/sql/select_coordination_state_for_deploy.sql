-- The locked confirming read the legacy deploy facade takes before touching
-- deploy_intent.
--
-- The column ORDER is load-bearing and is NOT the same as
-- select_coordination_state_actor.sql, which selects the same four columns as
-- (phase, generation, kind, requested_by). deploy.py reads this result
-- positionally -- row[0] is kind and row[1] is phase -- so the two statements
-- are kept apart deliberately. Reusing one for the other silently swaps them.
SELECT kind, phase, generation, requested_by
  FROM coordination_state WHERE id = 1
