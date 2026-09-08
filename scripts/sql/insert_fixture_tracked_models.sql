-- Seed one ops.tracked_models row for the CI fixture build.
--
-- Plan 162 Stage S. dbt has six sources and two of them are Postgres tables
-- read through postgres_scan; the lake-snapshot fixture seeder writes only to
-- MinIO, so this one arrived empty. int_active_make_models inner-joins it,
-- yields nothing, and mart_vehicle_snapshot inner-joins that -- so five of the
-- 23 models built over an empty world and every not_null test on them passed
-- vacuously. In production this table is written by the processing service as
-- makes and models appear in SRP results; nothing in the dbt path writes it,
-- which is why the fixture must.
--
-- The rows are not a second hand-kept list. Make and model come from the
-- fixture's own observations, lowercased to match what the processing service
-- writes and what int_active_make_models joins on, and search_key comes from
-- whichever configs are enabled -- so a make added to the fixture reaches the
-- marts without anyone editing this file.
--
-- Deliberately the same shape as the production upsert, conflict clause
-- included: presence is all that matters, and a fixture that seeds twice is a
-- no-op rather than an error.
--
-- The schema is interpolated because a relation name is a literal to the
-- driver, and that placeholder is also the testability seam -- Layer 2 points
-- it at a fixture schema instead of ops. The values are bound.
--
-- No braces in this comment beyond the placeholder: the whole file goes
-- through str.format.
INSERT INTO {schema}.tracked_models (search_key, make, model)
VALUES (%(search_key)s, LOWER(%(make)s), LOWER(%(model)s))
ON CONFLICT (search_key, make, model) DO NOTHING
