-- Drain evidence: artifacts currently admitted to processing.
--
-- Pending and retry rows are backlog, not admitted work, and never block a
-- drain -- only `processing` counts.
--
-- `oldest` prefers the moment the row entered `processing` over the moment it
-- was created, because a row that queued for an hour and started a minute ago
-- is a minute of admitted work, not an hour of it. COALESCE falls back to
-- created_at for rows that predate the event stream.
--
-- Every table is schema-qualified deliberately: the ops role's search_path is
-- `ops, staging, public`, and a drain that resolves a table by luck of naming
-- is one rename away from reporting `unknown`, which fails closed and hangs
-- the deploy.
SELECT COUNT(*), MIN(COALESCE(
           (SELECT MAX(event_at)
              FROM staging.artifacts_queue_events e
             WHERE e.artifact_id = q.artifact_id
               AND e.status = 'processing'),
           q.created_at))
  FROM ops.artifacts_queue q
 WHERE q.status = 'processing'
