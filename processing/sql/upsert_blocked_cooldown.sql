-- Upsert blocked_cooldown when a detail artifact is a 403 block page.
INSERT INTO ops.blocked_cooldown
    (listing_id, first_attempted_at, last_attempted_at, num_of_attempts)
VALUES
    (%(listing_id)s, now(), now(), 1)
ON CONFLICT (listing_id) DO UPDATE SET
    last_attempted_at = now(),
    num_of_attempts   = ops.blocked_cooldown.num_of_attempts + 1
