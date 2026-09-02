-- Read the silver observations a flush is about to write to MinIO.
-- The column list is filled in from the processor's _DB_COLUMNS. That list is
-- also what the returned tuples are zipped back against, so the column *order*
-- is load-bearing: retyping it here would give the projection a second
-- definition, free to drift from the one that names the values.
-- ORDER BY id keeps the Parquet row order matching the staging order.
SELECT {columns} FROM staging.silver_observations WHERE id <= %s ORDER BY id
