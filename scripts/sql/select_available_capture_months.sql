-- Distinct capture months present in the observation lake, oldest first.
--
-- Reads only the hive partition columns, so this is a directory listing rather
-- than a scan. The silver_path placeholder is interpolated for the same reason
-- the lake snapshot selectors interpolate their path: read_parquet takes a
-- literal, and the fixture-mode connection points it at local Parquet instead
-- of MinIO.
--
-- No braces in this comment beyond the placeholder itself: the whole file goes
-- through str.format, which reads a brace in a comment exactly as it reads one
-- in the statement.
SELECT DISTINCT
    printf('%04d-%02d', CAST(obs_year AS INTEGER), CAST(obs_month AS INTEGER))
        AS capture_month
FROM read_parquet('{silver_path}', hive_partitioning=true, union_by_name=true)
WHERE source ILIKE ?
  AND obs_year IS NOT NULL
  AND obs_month IS NOT NULL
ORDER BY capture_month
