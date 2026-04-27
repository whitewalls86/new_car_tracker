SELECT r.trigger,
       r.started_at AT TIME ZONE 'America/Chicago' AS started_at,
       ROUND(EXTRACT(EPOCH FROM now() - r.started_at) / 60) AS elapsed_min,
       r.progress_count,
       r.total_count,
       CASE WHEN r.total_count > 0
            THEN ROUND(r.progress_count::numeric /
                       (EXTRACT(EPOCH FROM now() - r.started_at) / 60), 1)
       END AS vins_per_min,
       (SELECT COUNT(*) FROM scrape_jobs j
        WHERE j.run_id = r.run_id AND j.status = 'failed') AS failed_jobs
FROM runs r
WHERE r.status = 'running'
ORDER BY r.started_at
