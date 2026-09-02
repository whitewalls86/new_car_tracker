-- The coordination row as the metrics exporter reads it.
-- Narrower than select_coordination_state.sql: the exporter needs scope and
-- updated_at, and none of the timestamps the API surfaces.
SELECT kind, phase, generation, scope, updated_at
  FROM coordination_state WHERE id = 1
