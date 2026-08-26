-- Plan 142 Stage 2: durable, append-only coordination transition history.
-- The current coordination row remains authoritative for the active window;
-- this table preserves each generation after that row is reused.

CREATE TABLE staging.coordination_state_events (
    event_id bigserial PRIMARY KEY,
    generation bigint NOT NULL CHECK (generation >= 1),
    prior_phase text NOT NULL
        CHECK (prior_phase IN ('none', 'requested', 'draining', 'active', 'validating')),
    phase text NOT NULL
        CHECK (phase IN ('none', 'requested', 'draining', 'active', 'validating')),
    kind text NOT NULL
        CHECK (kind IN ('deploy', 'service_maintenance', 'host_maintenance')),
    actor text NOT NULL CHECK (length(actor) > 0),
    event_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX coordination_state_events_generation_idx
    ON staging.coordination_state_events (generation, event_id);

GRANT SELECT ON staging.coordination_state_events TO viewer;
GRANT SELECT, INSERT, DELETE ON staging.coordination_state_events TO scraper_user;
GRANT USAGE, SELECT ON SEQUENCE staging.coordination_state_events_event_id_seq
    TO scraper_user;
