-- ---------------------------------------------------------------------------
-- V025: staging.silver_observations — buffer table for MinIO silver writes
--
-- Writers INSERT here instead of directly to MinIO. A scheduled DAG (or
-- eventually a Kafka consumer) flushes rows to partitioned Parquet in MinIO
-- and DELETEs the flushed rows.
--
-- Also creates the placeholder staging.silver_observation_events table.
-- ---------------------------------------------------------------------------

CREATE TABLE staging.silver_observations (
    id                    bigserial    PRIMARY KEY,

    -- identifiers & metadata
    artifact_id           bigint       NOT NULL,
    listing_id            text         NOT NULL,
    vin                   text,
    canonical_detail_url  text,
    source                text         NOT NULL CHECK (source IN ('detail', 'srp', 'carousel')),
    listing_state         text         NOT NULL DEFAULT 'active',
    fetched_at            timestamptz  NOT NULL,
    created_at            timestamptz  NOT NULL DEFAULT now(),

    -- core vehicle fields
    price                 integer,
    make                  text,
    model                 text,
    trim                  text,
    year                  smallint,
    mileage               integer,
    msrp                  integer,
    stock_type            text,
    fuel_type             text,
    body_style            text,

    -- dealer fields (detail + carousel)
    dealer_name           text,
    dealer_zip            text,
    customer_id           text,
    seller_id             text,
    dealer_street         text,
    dealer_city           text,
    dealer_state          text,
    dealer_phone          text,
    dealer_website        text,
    dealer_cars_com_url   text,
    dealer_rating         real,

    -- srp-specific fields
    financing_type        text,
    seller_zip            text,
    seller_customer_id    text,
    page_number           smallint,
    position_on_page      smallint,
    trid                  text,
    isa_context           text,

    -- carousel-specific fields
    body                  text,
    condition             text
);

-- Flush DAG queries by created_at order
CREATE INDEX silver_observations_created_at_idx
    ON staging.silver_observations (created_at);

-- ---------------------------------------------------------------------------
-- Placeholder event table for future Kafka / audit trail
-- ---------------------------------------------------------------------------

CREATE TABLE staging.silver_observation_events (
    event_id              bigserial    PRIMARY KEY,
    batch_id              text         NOT NULL,
    rows_flushed          integer      NOT NULL,
    source                text         NOT NULL,
    partition_date        date         NOT NULL,
    minio_path            text,
    event_type            text         NOT NULL CHECK (event_type IN ('flushed', 'failed')),
    event_at              timestamptz  NOT NULL DEFAULT now()
);

GRANT SELECT ON staging.silver_observations TO viewer;
GRANT SELECT ON staging.silver_observation_events TO viewer;
