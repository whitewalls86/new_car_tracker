SELECT listing_id FROM (  SELECT listing_id, arg_max(event_type, event_at) AS latest  FROM read_parquet(?, hive_partitioning=true)  GROUP BY listing_id) WHERE latest = 'cleared'
