-- Plans 97 + 98: MinIO-first artifact store + ops/staging schema layout
--
-- 1. staging schema       — append-only event buffers; flushed to MinIO Parquet
--                           by export DAG then truncated. Always small.
--
-- 2. ops.artifacts_queue  — Python-owned work queue; replaces raw_artifacts +
--                           artifact_processing as the live coordination table.
--                           raw_artifacts and artifact_processing stay in place
--                           during the shadow period and are dropped in Plan 90.
--
-- 3. staging.artifacts_queue_events
--                         — one row per status transition. No FK to ops table
--                           (staging row may outlive the cleaned-up hot row).
--
-- 4. ALTER ROLE            — sets search_path for all cartracker connections so
--                           unqualified table names resolve correctly:
--                           ops (hot tables) → staging (event buffers) → public.
--
-- 5. raw_artifacts.minio_path (nullable)
--                         — bridge column so n8n can carry the MinIO S3 URI
--                           through to /process/* endpoints during shadow period.

CREATE SCHEMA IF NOT EXISTS staging;

-- ---------------------------------------------------------------------------
-- ops.artifacts_queue
-- ---------------------------------------------------------------------------

CREATE TABLE ops.artifacts_queue (
    artifact_id   bigserial    PRIMARY KEY,
    minio_path    text         NOT NULL,
    artifact_type text         NOT NULL
                               CHECK (artifact_type IN ('results_page', 'detail_page')),
    listing_id    text,
    run_id        text,
    fetched_at    timestamptz  NOT NULL,
    status        text         NOT NULL DEFAULT 'pending'
                               CHECK (status IN ('pending', 'processing', 'complete', 'retry', 'skip')),
    created_at    timestamptz  NOT NULL DEFAULT now()
);

-- Partial index: only rows that need attention.
CREATE INDEX artifacts_queue_active_status_idx
    ON ops.artifacts_queue (status)
    WHERE status IN ('pending', 'retry');

-- ---------------------------------------------------------------------------
-- staging.artifacts_queue_events
-- ---------------------------------------------------------------------------

CREATE TABLE staging.artifacts_queue_events (
    event_id      bigserial    PRIMARY KEY,
    artifact_id   bigint       NOT NULL,
    status        text         NOT NULL,
    event_at      timestamptz  NOT NULL DEFAULT now(),
    minio_path    text,
    artifact_type text         NOT NULL,
    fetched_at    timestamptz,
    listing_id    text,
    run_id        text
);

CREATE INDEX artifacts_queue_events_artifact_id_idx
    ON staging.artifacts_queue_events (artifact_id);

-- ---------------------------------------------------------------------------
-- Search path
-- ---------------------------------------------------------------------------

-- All connections from the cartracker role resolve unqualified table names
-- against ops first, then staging, then public. No SQL query changes needed.
ALTER ROLE cartracker SET search_path TO ops, staging, public;

-- ---------------------------------------------------------------------------
-- Shadow-period bridge column
-- ---------------------------------------------------------------------------

-- raw_artifacts.minio_path carries the S3 URI written by the scraper so n8n
-- can pass it through to /process/* without reading artifacts_queue directly.
ALTER TABLE public.raw_artifacts
    ADD COLUMN minio_path text;
