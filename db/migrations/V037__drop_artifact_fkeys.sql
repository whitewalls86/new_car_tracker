-- V037: Drop FK constraints that reference ops.artifacts_queue
--
-- ops.vin_to_listing.artifact_id and ops.price_observations.last_artifact_id
-- both declare REFERENCES ops.artifacts_queue(artifact_id).  These are
-- informational audit-trail columns (which artifact created/last-updated the
-- row); they do not need referential integrity enforced at the DB level.
--
-- The cleanup_queue archiver job deletes completed/skipped rows from
-- artifacts_queue, but the FK prevents deletion while any vin_to_listing or
-- price_observations row still references the artifact.  Dropping the
-- constraints unblocks cleanup without changing application behaviour — the
-- columns remain and continue to be written.

ALTER TABLE ops.vin_to_listing
    DROP CONSTRAINT IF EXISTS vin_to_listing_artifact_id_fkey;

ALTER TABLE ops.price_observations
    DROP CONSTRAINT IF EXISTS price_observations_last_artifact_id_fkey;
