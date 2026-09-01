-- The full coordination record as /coordination/status reports it.
-- Wider than select_coordination_state_metrics.sql: this is the operator-facing
-- view, so it carries every phase timestamp and the request's own narrative.
SELECT kind, phase, generation, requested_by, reason, targets, scope, requested_at,
       draining_at, active_at, validating_at, completed_at, expected_work,
       manifest_location, operator_notes, updated_at
  FROM coordination_state
 WHERE id = 1
