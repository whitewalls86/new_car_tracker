-- Authorize a drained request. The phase and generation predicates are the
-- guard: the read that produced that generation was taken under the advisory
-- lock, so a row that moved underneath this update matches nothing, rowcount is
-- 0, and the caller is told it conflicted rather than being told it won.
UPDATE coordination_state
   SET phase = 'active', active_at = now(), updated_at = now()
 WHERE id = 1 AND phase = 'draining' AND generation = %s
