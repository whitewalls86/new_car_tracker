-- The locked confirming read taken before opening a request. Deliberately one
-- column narrower than select_coordination_state_actor.sql: /request has no
-- prior actor to attribute, and requested_by is about to be overwritten by the
-- new requester. The two are kept apart so neither can widen the other.
SELECT phase, generation, kind
  FROM coordination_state WHERE id = 1
