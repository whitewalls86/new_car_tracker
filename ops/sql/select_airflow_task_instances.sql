-- Drain evidence: active Airflow task instances among the drained tasks.
--
-- The caller passes the (dag_id, task_id) pairs its scope drains as two
-- parallel text arrays, and the active states as a third. Multi-argument
-- `unnest` zips the first two back into rows, so the pairs arrive as data.
--
-- **Static on purpose (Plan 162 Stage 9).** This was an f-string building a
-- VALUES list and an IN list, each sized to its arguments, and the contract's
-- "structurally generated statements" exemption was claimed for it. It never
-- qualified: only the *number* of values varied, never an identifier, and
-- array parameters are what that is for. Verified equivalent against
-- postgres:16 before the change.
--
-- Note that the placeholders cannot be spelled out in this comment: psycopg2
-- counts them as part of the statement, so a comment naming one makes the
-- caller pass too few parameters. That is its own contract rule.
--
-- The rewrite also removes a special case rather than adding one. `(VALUES )`
-- with no rows is a syntax error, which is why the builder used to return None
-- on an empty scope and every call site tested for it; `unnest` of an empty
-- array is legal and yields no rows, so an empty scope simply counts zero.
--
-- Schema-qualified deliberately -- see select_processing_artifacts_backlog.sql.
SELECT COUNT(*), MIN(ti.start_date)
  FROM airflow.task_instance ti
  JOIN unnest(%s::text[], %s::text[]) AS drained(dag_id, task_id)
    ON drained.dag_id = ti.dag_id
   AND drained.task_id = ti.task_id
 WHERE ti.state = ANY(%s)
