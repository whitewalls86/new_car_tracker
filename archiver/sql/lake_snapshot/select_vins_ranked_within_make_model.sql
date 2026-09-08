-- Take up to N VINs per make/model group, so a cohort spreads across the
-- catalogue instead of concentrating in whichever models happen to be busiest
-- (Plan 120 Gate C).
--
-- row_number() ORDER BY vin rather than a random function: the same input has
-- to produce the same cohort, or the export fingerprint stops meaning anything.
SELECT vin, listing_id
FROM (
    SELECT vin, listing_id,
           row_number() OVER (
               PARTITION BY concat_ws(' ', make, model) ORDER BY vin
           ) AS rn
    FROM read_parquet('{path}', union_by_name=true)
    WHERE {where_sql}
) AS ranked
WHERE rn <= {limit_per_group}
