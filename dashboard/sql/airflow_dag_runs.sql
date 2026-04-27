-- Recent Airflow DAG runs for the scrape DAGs.
-- Airflow uses the same cartracker Postgres DB; dag_run is in the public schema.
-- If this errors, the viewer role needs: GRANT SELECT ON dag_run TO viewer;
SELECT
    dag_id,
    run_id,
    state,
    start_date AT TIME ZONE 'America/Chicago' AS started,
    end_date AT TIME ZONE 'America/Chicago' AS ended,
    CASE
        WHEN end_date IS NOT NULL
            THEN ROUND(EXTRACT(EPOCH FROM (end_date - start_date)) / 60, 1)
        WHEN start_date IS NOT NULL
            THEN ROUND(EXTRACT(EPOCH FROM (now() - start_date)) / 60, 1)
    END AS duration_min,
    CASE WHEN end_date IS NULL AND start_date IS NOT NULL THEN true ELSE false END AS running
FROM dag_run
WHERE dag_id IN ('scrape_listings', 'scrape_detail_pages')
  AND start_date > now() - interval '3 days'
ORDER BY start_date DESC
LIMIT 50
