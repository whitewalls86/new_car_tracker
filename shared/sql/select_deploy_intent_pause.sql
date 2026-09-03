-- Does a pending deploy want long-running jobs to stop at their next boundary?
-- (Plan 131 Stage 5 D3b.)
--
-- Deliberately no staleness clause. A forgotten intent keeps long jobs paused
-- until it is released, the DAG exhausts its retries, and someone is paged --
-- which is the designed outcome, because an intent nobody cleared is a real
-- problem and not one a long job should paper over by starting anyway.
--
-- Row 1 is the only row; deploy_intent is a singleton table.
SELECT intent = 'pending' AND pause_long_jobs
FROM deploy_intent
WHERE id = 1
