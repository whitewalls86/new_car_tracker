-- Read the staging-event rows a flush is about to write to MinIO.
-- The relation, its primary key and the column list are filled in from
-- _TABLE_CONFIGS. The column list is the same db_columns the returned tuples
-- are zipped back against, so the column order is load-bearing and stays
-- defined once, in Python.
-- ORDER BY the primary key keeps the Parquet row order matching the staging
-- order.
SELECT {columns} FROM {table} WHERE {pk} <= %s ORDER BY {pk}
