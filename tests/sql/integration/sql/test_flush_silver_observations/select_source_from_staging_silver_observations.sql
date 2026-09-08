SELECT source FROM staging.silver_observations WHERE id <= (SELECT MAX(id) FROM staging.silver_observations)
