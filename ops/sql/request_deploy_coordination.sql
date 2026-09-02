-- The compatibility rollout's other half: having taken the legacy
-- deploy_intent lock, mirror it into coordination_state so new consumers see
-- the deploy. Narrower than request_coordination_state.sql -- the facade has no
-- expected_work, manifest or operator notes, and does not clear the previous
-- cycle's phase timestamps.
UPDATE coordination_state
   SET kind = 'deploy', phase = 'requested',
       generation = generation + 1,
       targets = %s::jsonb, scope = %s::jsonb,
       requested_by = %s, reason = 'Legacy deploy facade',
       requested_at = now(), updated_at = now()
 WHERE id = 1
RETURNING generation
