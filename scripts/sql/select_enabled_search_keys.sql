-- The search keys whose configs are enabled, for the CI fixture seed.
--
-- Plan 162 Stage S. int_active_make_models joins tracked_models to
-- stg_search_configs on search_key and filters to enabled = true, so a
-- tracked_models row carrying a key that is disabled or absent contributes
-- nothing and the fixture would still build five models over an empty world.
-- Reading the keys back rather than hardcoding one is what keeps the seed
-- correct when the migration that populates search_configs changes.
--
-- The schema is interpolated because a relation name is a literal to the
-- driver, and is the seam Layer 2 uses to point this at a fixture schema.
--
-- No braces in this comment beyond the placeholder: the whole file goes
-- through str.format.
SELECT search_key
FROM {schema}.search_configs
WHERE enabled = true
ORDER BY search_key
