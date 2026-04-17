UPDATE artifact_processing
SET
    status  = 'retry',
    message = 'Terminated: exceeded ' || %s || '-minute timeout'
WHERE
    status = 'processing'
    AND processed_at < now() - (interval '1 minute' * %s)
RETURNING artifact_id
