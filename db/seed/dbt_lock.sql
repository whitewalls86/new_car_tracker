-- dbt_lock: single-row mutex to prevent concurrent dbt builds.
-- The CHECK constraint enforces exactly one row.

CREATE TABLE IF NOT EXISTS dbt_lock (
    id          int PRIMARY KEY DEFAULT 1,
    locked      boolean NOT NULL DEFAULT false,
    locked_at   timestamptz,
    locked_by   text,
    CONSTRAINT single_row CHECK (id = 1)
);

INSERT INTO dbt_lock (id, locked, locked_at, locked_by)
VALUES (1, false, NULL, NULL)
ON CONFLICT (id) DO NOTHING;
