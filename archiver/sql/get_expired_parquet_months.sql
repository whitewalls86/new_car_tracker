SELECT DISTINCT
    EXTRACT(YEAR FROM archived_at)::int  AS year,
    EXTRACT(MONTH FROM archived_at)::int AS month
FROM raw_artifacts
WHERE archived_at IS NOT NULL
  AND deleted_at IS NULL
  AND archived_at < now() - interval '28 days'
ORDER BY year, month
