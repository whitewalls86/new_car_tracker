-- Wrap a selector's candidate-row query so counting and bounded sampling
-- happen in DuckDB rather than by pulling every candidate row into archiver
-- memory (Plan 120 Gate C).
--
-- `candidate_sql` is another query, not a value: it is the selector's own
-- statement, itself loaded from archiver/sql/lake_snapshot_selectors/. That is
-- what cannot be a bind parameter here. `entity_key` is a column name and
-- `cap` a LIMIT, neither of which SQL parameterises either.
WITH selector_candidates AS (
{candidate_sql}
),
distinct_entities AS (
    SELECT DISTINCT {entity_key} AS entity_value
    FROM selector_candidates
    WHERE {entity_key} IS NOT NULL
)
SELECT
    (SELECT count(*) FROM selector_candidates) AS candidate_rows,
    (SELECT count(*) FROM distinct_entities) AS entities,
    (
        SELECT list(entity_value)
        FROM (
            SELECT entity_value
            FROM distinct_entities
            ORDER BY entity_value
            LIMIT {cap}
        ) AS bounded
    ) AS bounded_entities
