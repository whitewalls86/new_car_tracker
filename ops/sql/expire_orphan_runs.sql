UPDATE runs
SET
    status      = 'terminated',
    finished_at = now(),
    error_count = COALESCE(error_count, 0) + 1,
    last_error  = 'Terminated: exceeded ' || %s || '-minute timeout'
WHERE
    status = 'running'
    AND started_at < now() - (interval '1 minute' * %s)
RETURNING run_id
