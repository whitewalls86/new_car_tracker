-- Release the legacy deploy_intent lock. Unconditional on the current intent:
-- the caller has already decided the release is allowed by reading
-- coordination_state, so a no-row result here means the singleton is missing,
-- not that the release was refused.
UPDATE deploy_intent
   SET
       intent = 'none',
       requested_at = NULL,
       requested_by = NULL
   WHERE id = 1
   RETURNING intent;
