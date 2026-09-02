-- Enqueue a fetched search-results page for the processing service (Plan 97).
-- Positional params: (minio_path, run_id, fetched_at, search_key).
-- search_key (added by V024) is populated for results_page only; the detail-page
-- sibling carries listing_id in its place, which is why the two statements stay
-- separate files rather than becoming one parameterised statement.
INSERT INTO ops.artifacts_queue
    (minio_path, artifact_type, run_id,
     fetched_at, status, search_key)
VALUES (%s, 'results_page', %s, %s, 'pending', %s)
RETURNING artifact_id
