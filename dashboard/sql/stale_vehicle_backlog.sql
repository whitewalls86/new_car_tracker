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
first_part AS (
    SELECT
        CASE
            WHEN priority_row < 601  THEN '00_next_batch'
            WHEN priority_row < 1201 THEN '01_following_batch'
            WHEN priority_row < 1801 THEN '02_third_batch'
            ELSE '03_backlog'
        END AS batch_param,
        COUNT(*) FILTER (WHERE stale_reason LIKE 'price_only%')::varchar AS price_only,
        COUNT(*) FILTER (WHERE stale_reason LIKE 'force_stale_36h%')::varchar AS force_stale,
        COUNT(*) FILTER (WHERE stale_reason LIKE 'full_details%')::varchar AS full_details,
        COUNT(*) FILTER (WHERE stale_reason LIKE 'unmapped_carousel')::varchar AS unmapped_carousel,
        COUNT(*) FILTER (WHERE stale_reason LIKE 'dealer_unenriched')::varchar AS dealer_unenriched,
        COUNT(*)::varchar AS total_count
    FROM batch_marking q
    GROUP BY 1
    ORDER BY batch_param ASC
),
second_part AS (
    SELECT
        'Grand Total' AS batch_param,
        COUNT(*) FILTER (WHERE stale_reason LIKE 'price_only%')::varchar AS price_only,
        COUNT(*) FILTER (WHERE stale_reason LIKE 'force_stale_36h%')::varchar AS force_stale,
        COUNT(*) FILTER (WHERE stale_reason LIKE 'full_details%')::varchar AS full_details,
        COUNT(*) FILTER (WHERE stale_reason LIKE 'unmapped_carousel%')::varchar AS unmapped_carousel,
        COUNT(*) FILTER (WHERE stale_reason LIKE 'dealer_unenriched%')::varchar AS dealer_unenriched,
        COUNT(*)::varchar AS total_count
    FROM batch_marking q
    GROUP BY 1
)
SELECT * FROM first_part
UNION ALL
SELECT
    '----------' AS batch_param,
    '----------' AS price_only,
    '----------' AS force_stale,
    '----------' AS full_details,
    '----------' AS unmapped_carousel,
    '----------' AS dealer_unenriched,
    '----------' AS total_count
UNION ALL
SELECT * FROM second_part
