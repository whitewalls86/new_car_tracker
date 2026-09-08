SELECT now() - fetched_at AS age FROM staging.silver_observations WHERE artifact_id = %s LIMIT 1
