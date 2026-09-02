-- Record that a DAG run has seen the coordination gate and is holding.
-- ops.coordination_drain's gate_observation_query counts these to decide when
-- a drain is complete, so a run that holds without recording is invisible to it.
-- Upsert on re-entry: a sensor that pokes repeatedly must refresh observed_at,
-- not accumulate rows.
INSERT INTO coordination_gate_observations
       (generation, dag_id, run_id, observed_at)
VALUES (%s, %s, %s, now())
ON CONFLICT (generation, dag_id, run_id)
DO UPDATE SET observed_at = EXCLUDED.observed_at
