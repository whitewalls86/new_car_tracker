SELECT event_id, listing_id, event_type, num_of_attempts, event_at
FROM read_parquet('{path}', union_by_name=true)
{where}
ORDER BY listing_id, event_at
