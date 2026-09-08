-- Sample detail artifacts evenly across the requested capture months.
--
-- Ordering is by hash(artifact_id) rather than a shuffle, so a re-run with the
-- same arguments returns the same artifacts: a storage decision that cannot be
-- reproduced cannot be audited. The name of the shuffling function is left out
-- of this comment on purpose -- test_corpus_sample_spreads_the_budget_across_
-- months_and_is_deterministic asserts it appears nowhere in the statement, and
-- since Stage X the statement is this whole file, comments included.
--
-- Four interpolations, each for a reason a bind parameter cannot serve:
--   silver_path, artifact_events_path   read_parquet takes a literal
--   month_placeholders                  one ? per month, so the count is part
--                                       of the statement's shape
--   per_month                           cast to int at the call site, as
--                                       wrap_candidate_query.sql does with its
--                                       own cap
-- The source pattern and the month values are bound, not interpolated.
--
-- No braces in this comment beyond the placeholders themselves: the whole file
-- goes through str.format, which reads a brace in a comment exactly as it reads
-- one in the statement.
WITH obs AS (
    SELECT
        artifact_id,
        listing_id,
        fetched_at,
        printf('%04d-%02d', CAST(obs_year AS INTEGER), CAST(obs_month AS INTEGER))
            AS capture_month
    FROM read_parquet('{silver_path}', hive_partitioning=true, union_by_name=true)
    WHERE source ILIKE ?
      AND listing_id IS NOT NULL
      AND artifact_id IS NOT NULL
),

in_window AS (
    SELECT * FROM obs WHERE capture_month IN ({month_placeholders})
),

one_row_per_artifact AS (
    SELECT
        artifact_id,
        any_value(listing_id) AS listing_id,
        any_value(capture_month) AS capture_month,
        min(fetched_at) AS fetched_at
    FROM in_window
    GROUP BY artifact_id
),

ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY capture_month
            ORDER BY hash(artifact_id)
        ) AS month_rank
    FROM one_row_per_artifact
),

artifact_paths AS (
    SELECT
        artifact_id,
        any_value(minio_path) AS minio_path
    FROM read_parquet('{artifact_events_path}', hive_partitioning=true, union_by_name=true)
    WHERE artifact_type = 'detail_page'
      AND minio_path IS NOT NULL
    GROUP BY artifact_id
)

SELECT
    r.artifact_id,
    r.listing_id,
    r.capture_month,
    r.fetched_at,
    p.minio_path
FROM ranked r
JOIN artifact_paths p USING (artifact_id)
WHERE r.month_rank <= {per_month}
ORDER BY r.capture_month, r.month_rank
