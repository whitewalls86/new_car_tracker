-- V036: Drop raw artifact tables.
-- PREREQUISITE: Track 4 (archiver cleanup) must be deployed and validated before
-- running this migration. The archiver must no longer read raw_artifacts.

DROP TABLE IF EXISTS public.artifact_processing;  -- no FK deps remain after V035
DROP TABLE IF EXISTS public.raw_artifacts CASCADE;
