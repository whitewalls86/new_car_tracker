SELECT COUNT(*) AS cnt FROM ops.price_observations WHERE last_artifact_id = %s AND vin IS NOT NULL
