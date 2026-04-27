WITH batch_marking AS (
    SELECT
        q.listing_id,
        q.stale_reason,
        ROW_NUMBER() OVER (
            PARTITION BY 1 ORDER BY q.priority, q.listing_id
        ) AS priority_row
    FROM ops.ops_detail_scrape_queue q
    LEFT JOIN ops.detail_scrape_claims c
        ON c.listing_id = q.listing_id AND c.status = 'running'
    WHERE c.listing_id IS NULL
),
cooldown_eligible AS (
    SELECT
        bc.listing_id,
        bc.num_of_attempts,
        bc.last_attempted_at + (
            interval '1 hour' * (12 * power(2, bc.num_of_attempts::float - 1))
        ) AS next_eligible_at
    FROM ops.blocked_cooldown bc
)
SELECT
    ce.num_of_attempts,
    MIN(ce.next_eligible_at) FILTER (
        WHERE ce.next_eligible_at > now()
    ) AT TIME ZONE 'America/Chicago' AS next_attempt_at,
    COUNT(ce.listing_id) AS num_listings,
    COUNT(ce.listing_id) FILTER (
        WHERE ce.next_eligible_at < now()
        AND ovs.stale_reason != 'not_stale'
    ) AS eligible_now,
    COUNT(ce.listing_id) FILTER (
        WHERE q.priority_row < 601 AND q.priority_row IS NOT NULL
    ) AS num_in_next_batch
FROM cooldown_eligible ce
LEFT JOIN batch_marking q ON q.listing_id = ce.listing_id
LEFT JOIN ops.ops_vehicle_staleness ovs ON ce.listing_id = ovs.listing_id
GROUP BY ce.num_of_attempts
ORDER BY ce.num_of_attempts
