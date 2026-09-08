SELECT count(*) AS n FROM staging.silver_observations WHERE artifact_id = ANY(%s)
