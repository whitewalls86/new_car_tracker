-- Plan 89: Application-owned operational tables
-- Replaces dbt intermediate models (int_listing_to_vin, int_price_events,
-- int_latest_tier1_observation_by_vin) with write-path tables owned by the
-- processing service. These are populated during the shadow period alongside
-- the existing dbt models; dbt cleanup happens in Plan 90.

-- ── listing_to_vin ─────────────────────────────────────────────────────────
-- Authoritative listing_id → vin mapping. Written by the processing service
-- on every SRP or detail observation that carries a valid VIN.
-- Upsert strategy: recency-only — most recent observation wins.

CREATE TABLE public.listing_to_vin (
    listing_id          uuid        NOT NULL,
    vin                 text        NOT NULL,
    vin_observed_at     timestamptz NOT NULL,
    vin_artifact_id     bigint      NOT NULL,
    CONSTRAINT listing_to_vin_pkey PRIMARY KEY (listing_id),
    CONSTRAINT listing_to_vin_artifact_id_fkey
        FOREIGN KEY (vin_artifact_id) REFERENCES public.raw_artifacts (artifact_id)
);

CREATE INDEX ix_listing_to_vin_vin ON public.listing_to_vin (vin);


-- ── price_observations ─────────────────────────────────────────────────────
-- Append-only log of every price signal seen, from any source.
-- vin is nullable: carousel hints may not be mapped to a VIN yet.
-- source: 'srp' | 'detail' | 'carousel'

CREATE TABLE public.price_observations (
    id              bigserial   PRIMARY KEY,
    listing_id      uuid        NOT NULL,
    vin             text,
    price           integer     NOT NULL,
    observed_at     timestamptz NOT NULL,
    artifact_id     bigint      NOT NULL,
    source          text        NOT NULL,
    CONSTRAINT price_observations_artifact_id_fkey
        FOREIGN KEY (artifact_id) REFERENCES public.raw_artifacts (artifact_id)
);

CREATE INDEX ix_price_obs_listing_id_observed_at
    ON public.price_observations (listing_id, observed_at DESC);

CREATE INDEX ix_price_obs_vin_observed_at
    ON public.price_observations (vin, observed_at DESC)
    WHERE vin IS NOT NULL;


-- ── vin_state ──────────────────────────────────────────────────────────────
-- Authoritative current state per VIN: listing location, listing state,
-- mileage, dealer IDs. Both SRP and detail observations write here.
-- Detail-only fields (listing_state, customer_id, mileage) use COALESCE
-- upsert logic so SRP writes never overwrite real data with NULL.

CREATE TABLE public.vin_state (
    vin                     text        NOT NULL,
    listing_id              uuid,
    listing_state           text,
    mileage                 integer,
    canonical_detail_url    text,
    seller_customer_id      text,
    customer_id             text,
    state_observed_at       timestamptz NOT NULL,
    state_artifact_id       bigint      NOT NULL,
    CONSTRAINT vin_state_pkey PRIMARY KEY (vin),
    CONSTRAINT vin_state_artifact_id_fkey
        FOREIGN KEY (state_artifact_id) REFERENCES public.raw_artifacts (artifact_id)
);

CREATE INDEX ix_vin_state_listing_id ON public.vin_state (listing_id)
    WHERE listing_id IS NOT NULL;
