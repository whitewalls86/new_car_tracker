UPDATE scrape_jobs
SET
    status = 'failed',
    error  = 'Timeout — job lost, likely due to container restart'
WHERE status IN ('queued', 'running')
  AND run_id NOT IN (
      SELECT run_id FROM runs WHERE status = 'running'
  )
RETURNING job_id
