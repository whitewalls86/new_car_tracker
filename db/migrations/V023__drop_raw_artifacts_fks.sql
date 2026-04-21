-- V023: Drop FK constraints that reference public.raw_artifacts
--
-- raw_artifacts is being deprecated (Plan 90). The processing service now
-- writes artifact_ids from ops.artifacts_queue (a HOT table), so these FK
-- constraints cannot be repointed — the HOT table cleans up old rows.
-- Drop them entirely; the processing service owns referential correctness.

ALTER TABLE public.detail_carousel_hints
    DROP CONSTRAINT detail_carousel_hints_artifact_id_fkey;

ALTER TABLE public.detail_observations
    DROP CONSTRAINT detail_observations_artifact_id_fkey;

ALTER TABLE public.srp_observations
    DROP CONSTRAINT srp_observations_artifact_id_fkey;

ALTER TABLE public.artifact_processing
    DROP CONSTRAINT artifact_processing_artifact_id_fkey;
