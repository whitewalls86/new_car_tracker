WITH obs AS (
    -- {where} applies the bounded lookback (window_end - lookback_days ..
    -- window_end) when window_end is supplied, or no filter at all for an
    -- unbounded/no-window call — never the normal [window_start, window_end)
    -- filter, which would exclude exactly the pre-window_start history this
    -- selector needs to answer "what was the last observation as of
    -- window_end".
    SELECT listing_id, vin, artifact_id, fetched_at
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
last_row AS (
    -- The exact (vin, artifact_id) of the row that establishes last_seen_at
    -- for each listing — deterministic tie-break by artifact_id when two
    -- rows share the same fetched_at. Exposed so the cohort/export layer can
    -- capture this row as an exact row-key export exemption: it may predate
    -- window_start, so a blanket [window_start, window_end) export filter
    -- would otherwise drop it, leaving the listing with zero supporting
    -- rows despite being selected.
    SELECT listing_id, vin, artifact_id, fetched_at,
           row_number() OVER (
               PARTITION BY listing_id ORDER BY fetched_at DESC, artifact_id
           ) AS rn
    FROM obs
),
anchored AS (
    -- Explicit window_end is the as-of anchor. The MAX(last_seen_at)
    -- fallback only applies when no window_end is given at all (an
    -- unbounded/no-window call); it must never override an explicit
    -- window_end with "now" or "whichever row happens to be newest".
    SELECT agg.listing_id, agg.first_seen_at, agg.last_seen_at,
           last_row.vin, last_row.artifact_id,
           COALESCE(CAST(? AS TIMESTAMP), max(agg.last_seen_at) OVER ()) AS window_end
    FROM agg
    JOIN last_row ON last_row.listing_id = agg.listing_id AND last_row.rn = 1
)
SELECT listing_id, vin, artifact_id, first_seen_at, last_seen_at, window_end
FROM anchored
WHERE window_end - last_seen_at >= INTERVAL 30 DAY
ORDER BY listing_id
