-- Wrap a selector's candidate-row query so counting and sampling happen in
-- DuckDB rather than by pulling every candidate row into archiver memory
-- (Plan 120 Gate B).
--
-- The sibling of wrap_candidate_query.sql, which answers the same question for
-- the cohort builder. This one is the selector diagnostics' version: it reports
-- a five-entity sample rather than a caller-capped entity list.
--
-- `candidate_sql` is a query and `entity_key` a column name; neither is a
-- value SQL can bind.
WITH selector_candidates AS (
{candidate_sql}
)
SELECT
    count(*) AS candidate_rows,
    count(DISTINCT {entity_key}) AS entities,
    (
        SELECT list(entity_value)
        FROM (
            SELECT DISTINCT {entity_key} AS entity_value
            FROM selector_candidates
            WHERE {entity_key} IS NOT NULL
            ORDER BY entity_value
            LIMIT 5
        ) AS sample
    ) AS sample_entities
FROM selector_candidates
