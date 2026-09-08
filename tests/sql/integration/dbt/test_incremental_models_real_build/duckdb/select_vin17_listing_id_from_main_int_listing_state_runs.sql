select vin17, listing_id, parsed_fingerprint, run_started_at, run_ended_at, artifact_count, hours_until_change, is_open_run from main.int_listing_state_runs where vin17 = ? order by run_started_at
