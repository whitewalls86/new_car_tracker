-- Drain evidence: active runs of affected DAGs that have not seen the gate.
--
-- A run that holds at the coordination gate records itself through
-- airflow/sql/record_gate_observation.sql. This counts the runs that are still
-- active and have *not* recorded for the current generation -- the ones whose
-- position is unknown -- and the drain refuses to authorize while it is
-- non-zero. `generation` is what stops an observation from an earlier drain
-- authorizing this one.
--
-- The affected dag_ids arrive as a text array; see
-- select_airflow_task_instances.sql for why this is static rather than an
-- f-string, and for what the rewrite deleted.
SELECT COUNT(*), MIN(dr.start_date)
  FROM airflow.dag_run dr
  JOIN unnest(%s::text[]) AS affected(dag_id)
    ON affected.dag_id = dr.dag_id
 WHERE dr.state IN ('queued', 'running')
   AND NOT EXISTS (
       SELECT 1
         FROM public.coordination_gate_observations observed
        WHERE observed.generation = %s
          AND observed.dag_id = dr.dag_id
          AND observed.run_id = dr.run_id
   )
