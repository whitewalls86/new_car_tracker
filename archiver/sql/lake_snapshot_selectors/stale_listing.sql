WITH obs AS (
    -- {where} applies the bounded lookback (window_end - lookback_days ..
    -- window_end) when window_end is supplied, or no filter at all for an
    -- unbounded/no-window call — never the normal [window_start, window_end)
    -- filter, which would exclude exactly the pre-window_start history this
    -- selector needs to answer "what was the last observation as of
    -- window_end".
    SELECT listing_id, fetched_at
    FROM read_parquet('{path}', union_by_name=true)
    {where}
),
agg AS (
    SELECT listing_id,
           min(fetched_at) AS first_seen_at,
           max(fetched_at) AS last_seen_at
    FROM obs
    GROUP BY listing_id
),
anchored AS (
    -- Explicit window_end is the as-of anchor. The MAX(last_seen_at)
    -- fallback only applies when no window_end is given at all (an
    -- unbounded/no-window call); it must never override an explicit
    -- window_end with "now" or "whichever row happens to be newest".
    SELECT *, COALESCE(CAST(? AS TIMESTAMP), max(last_seen_at) OVER ()) AS window_end
    FROM agg
)
SELECT listing_id, first_seen_at, last_seen_at, window_end
FROM anchored
WHERE window_end - last_seen_at >= INTERVAL 30 DAY
ORDER BY listing_id
