SELECT count(*) AS count FROM staging.coordination_state_events WHERE generation=%s AND prior_phase='validating' AND phase='none'
