-- Plan 145 Stage 5 slice 2: a durable receipt for every committed recovery
-- batch, written inside the same transaction as the rows it describes.
--
-- Why this table has to exist. All three tables the recovery writer touches
-- are asynchronously flushed to the lake and then DELETEd
-- (archiver/processors/flush_silver_observations.py deletes the rows it
-- flushed). So after an ambiguous client response, querying PostgreSQL cannot
-- distinguish "never committed" from "committed and already flushed away", and
-- a retry driven off row counts would double-write history. The receipt is the
-- only durable evidence, which is why retry is keyed on it.
--
-- Follows V046__coordination_completion_receipts.sql, the house pattern for
-- exactly this problem: authoritative operational state, not a flushable
-- staging event.
--
-- Retry semantics the writer enforces against this table:
--   * same batch_name + same assignment-manifest digest -> skip, write zero rows;
--   * same batch_name + a different digest              -> stop, surface both.
-- The composite primary key makes the second case observable rather than an
-- overwrite.

CREATE TABLE public.plan145_recovery_batch_receipts (
    batch_name        text        NOT NULL,
    manifest_sha256   text        NOT NULL CHECK (length(manifest_sha256) = 64),
    artifact_count    integer     NOT NULL,
    silver_count      integer     NOT NULL,
    price_event_count integer     NOT NULL,
    queue_event_count integer     NOT NULL,
    committed_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_name, manifest_sha256)
);

-- The writer runs from the april-processor compose profile, which connects as
-- `cartracker` -- the role production's own processing service uses for the
-- three staging writes this receipt accompanies. `scraper_user` is not granted
-- INSERT here on purpose: it has only SELECT, DELETE on
-- staging.price_observation_events (V027) and no INSERT grant anywhere in
-- db/migrations, so granting it the receipt alone would half-enable a path
-- that cannot complete. Changing the writer role is a separate migration and a
-- separate decision.
GRANT SELECT, INSERT ON public.plan145_recovery_batch_receipts TO cartracker;
GRANT SELECT ON public.plan145_recovery_batch_receipts TO scraper_user;
GRANT SELECT ON public.plan145_recovery_batch_receipts TO viewer;
