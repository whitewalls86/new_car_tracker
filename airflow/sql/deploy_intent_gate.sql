-- The admission read behind `deploy_intent_sensor`: may this DAG run start?
--
-- Two independent holds, read in one round trip because a sensor pokes every
-- 60 seconds for every gated DAG and two queries would double that load for
-- no added signal:
--
--   intent      the legacy deploy flag, 'none' when no deploy is in flight
--   phase       the scoped coordination phase
--   intersects  whether this run's admission surfaces are inside the deploy's
--               scope -- true for a whole-host deploy, or when any surface the
--               caller passes appears in `cs.scope`
--   generation  the drain generation, the key half of the observation row
--               `record_gate_observation.sql` writes when the run holds
--
-- **Column order is load-bearing.** `_DeployIntentSensor.poke` reads this row
-- positionally (`row[0]`..`row[3]`) because `PostgresHook.get_first` returns a
-- tuple, so reordering the select list silently changes which value gates
-- admission -- `row[0] != "none"` against `phase` would admit runs during a
-- deploy. tests/integration/sql/test_airflow_dag_queries.py asserts the order
-- against a real Postgres so that change fails in CI instead of in production.
--
-- The parameter is the DAG's admission surfaces as a text[]; `?` and `?|` are
-- jsonb operators here, not placeholders.
SELECT di.intent, cs.phase,
       cs.scope ? 'host' OR cs.scope ?| %s::text[] AS intersects,
       cs.generation
  FROM deploy_intent di
 CROSS JOIN coordination_state cs
 WHERE di.id = 1 AND cs.id = 1
