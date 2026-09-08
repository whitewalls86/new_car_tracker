INSERT INTO ops.blocked_cooldown
    (listing_id, first_attempted_at, last_attempted_at, num_of_attempts)
VALUES (
    %s::uuid,
    now() - interval '7 days',
    now() - (%s || ' hours')::interval,
    %s
)
