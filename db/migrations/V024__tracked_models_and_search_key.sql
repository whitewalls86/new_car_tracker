-- V024: Carousel model-level filtering
--
-- Adds search_key to artifacts_queue so the processing service knows which
-- search config produced an SRP artifact. Creates ops.tracked_models (HOT
-- table) populated during SRP processing with the actual make/model pairs
-- observed per search_key. Carousel hints are filtered against this table
-- joined to enabled search_configs — replacing the regex-based make-only
-- filter from the initial Plan 93 implementation.
--
-- 1. artifacts_queue.search_key — nullable; populated for results_page only
-- 2. ops.tracked_models — presence table: (search_key, make, model)
-- 3. staging.tracked_model_events — append-only audit trail

-- ---------------------------------------------------------------------------
-- 1. Add search_key to artifacts_queue
-- ---------------------------------------------------------------------------

ALTER TABLE ops.artifacts_queue
    ADD COLUMN search_key text;

-- ---------------------------------------------------------------------------
-- 2. ops.tracked_models — HOT table
-- ---------------------------------------------------------------------------

CREATE TABLE ops.tracked_models (
    search_key   text   NOT NULL,
    make         text   NOT NULL,
    model        text   NOT NULL,
    PRIMARY KEY (search_key, make, model)
);

-- ---------------------------------------------------------------------------
-- 3. staging.tracked_model_events
-- ---------------------------------------------------------------------------

CREATE TABLE staging.tracked_model_events (
    event_id     bigserial    PRIMARY KEY,
    search_key   text         NOT NULL,
    make         text         NOT NULL,
    model        text         NOT NULL,
    event_type   text         NOT NULL CHECK (event_type IN ('added', 'removed')),
    event_at     timestamptz  NOT NULL DEFAULT now()
);

CREATE INDEX tracked_model_events_search_key_idx
    ON staging.tracked_model_events (search_key);

CREATE INDEX tracked_model_events_event_at_idx
    ON staging.tracked_model_events (event_at);

-- ---------------------------------------------------------------------------
-- 4. Grants
-- ---------------------------------------------------------------------------

GRANT SELECT ON ops.tracked_models TO viewer;
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO viewer;
