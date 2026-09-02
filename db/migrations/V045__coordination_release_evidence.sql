-- Plan 142 Stage 3: durable, append-only host validation evidence.
-- Rows are intentionally never updated: a later submission documents a later
-- observation while generation prevents old evidence authorizing new work.

CREATE TABLE staging.coordination_release_evidence (
    evidence_id bigserial PRIMARY KEY,
    generation bigint NOT NULL CHECK (generation >= 1),
    actor text NOT NULL CHECK (length(actor) > 0),
    submitted_at timestamptz NOT NULL DEFAULT now(),
    gate_results jsonb NOT NULL,
    evidence_digests jsonb NOT NULL
);

CREATE INDEX coordination_release_evidence_generation_idx
    ON staging.coordination_release_evidence (generation, evidence_id);

GRANT SELECT ON staging.coordination_release_evidence TO viewer;
GRANT SELECT, INSERT, DELETE ON staging.coordination_release_evidence TO scraper_user;
GRANT USAGE, SELECT ON SEQUENCE staging.coordination_release_evidence_evidence_id_seq
    TO scraper_user;
