-- Snapshot boundary for a silver-observations flush.
-- The flush writes and then deletes everything at or below this id, so taking
-- the boundary up front is what keeps rows arriving mid-flush out of the batch.
SELECT MAX(id) FROM staging.silver_observations
