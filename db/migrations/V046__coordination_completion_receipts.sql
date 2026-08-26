-- Plan 142 Stage 3: retain a replay-confirmable receipt after completion clears
-- the active coordination row.  This is authoritative operational state, not a
-- flushable staging event, so an ambiguous client response remains recoverable.

CREATE TABLE public.coordination_completion_receipts (
    generation bigint NOT NULL CHECK (generation >= 1),
    manifest_sha256 text NOT NULL CHECK (length(manifest_sha256) = 64),
    completed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (generation, manifest_sha256)
);

GRANT SELECT, INSERT ON public.coordination_completion_receipts TO scraper_user;
GRANT SELECT ON public.coordination_completion_receipts TO viewer;
