--
-- PostgreSQL database dump
--

\restrict EI4S9eTNd5vfIaQiFadHRaRgcHpdKTjHhvdvl5SZ7M8yV5Ei5gpX8VdpMk70EXk

-- Dumped from database version 16.11 (Debian 16.11-1.pgdg13+1)
-- Dumped by pg_dump version 16.11 (Debian 16.11-1.pgdg13+1)

-- Started on 2026-01-28 14:19:48 UTC

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 219 (class 1259 OID 16427)
-- Name: artifact_processing; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.artifact_processing (
    artifact_id bigint NOT NULL,
    processor text NOT NULL,
    status text NOT NULL,
    processed_at timestamp with time zone DEFAULT now() NOT NULL,
    message text,
    meta jsonb
);


--
-- TOC entry 227 (class 1259 OID 41034)
-- Name: detail_carousel_hints; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.detail_carousel_hints (
    id bigint NOT NULL,
    artifact_id bigint NOT NULL,
    fetched_at timestamp with time zone NOT NULL,
    source_listing_id text,
    listing_id text NOT NULL,
    price integer,
    mileage integer,
    body text,
    condition text,
    year integer
);


--
-- TOC entry 226 (class 1259 OID 41033)
-- Name: detail_carousel_hints_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.detail_carousel_hints_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 3559 (class 0 OID 0)
-- Dependencies: 226
-- Name: detail_carousel_hints_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.detail_carousel_hints_id_seq OWNED BY public.detail_carousel_hints.id;


--
-- TOC entry 225 (class 1259 OID 41015)
-- Name: detail_observations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.detail_observations (
    id bigint NOT NULL,
    artifact_id bigint NOT NULL,
    fetched_at timestamp with time zone NOT NULL,
    listing_id text,
    vin text,
    listing_state text DEFAULT 'active'::text NOT NULL,
    price integer,
    mileage integer,
    msrp integer,
    stock_type text,
    dealer_name text,
    dealer_zip text
);


--
-- TOC entry 224 (class 1259 OID 41014)
-- Name: detail_observations_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.detail_observations_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 3560 (class 0 OID 0)
-- Dependencies: 224
-- Name: detail_observations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.detail_observations_id_seq OWNED BY public.detail_observations.id;


--
-- TOC entry 230 (class 1259 OID 41460)
-- Name: listing_current_state; Type: MATERIALIZED VIEW; Schema: public; Owner: -
--

CREATE MATERIALIZED VIEW public.listing_current_state AS
 SELECT DISTINCT ON (listing_id) listing_id,
    listing_state,
    fetched_at AS listing_state_seen_at,
    artifact_id AS listing_state_artifact_id
   FROM public.detail_observations d
  WHERE (listing_id IS NOT NULL)
  ORDER BY listing_id, fetched_at DESC, id DESC
  WITH NO DATA;


--
-- TOC entry 232 (class 1259 OID 41526)
-- Name: listing_current_state_v; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.listing_current_state_v AS
 SELECT DISTINCT ON (listing_id) listing_id,
    listing_state,
    fetched_at AS listing_state_seen_at,
    artifact_id AS listing_state_artifact_id
   FROM public.detail_observations d
  WHERE ((listing_id IS NOT NULL) AND (listing_id <> ''::text))
  ORDER BY listing_id, fetched_at DESC, id DESC;


--
-- TOC entry 220 (class 1259 OID 16441)
-- Name: listings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.listings (
    listing_id text NOT NULL,
    source text DEFAULT 'cars.com'::text NOT NULL,
    vin text,
    vin_resolved_at timestamp with time zone,
    resolution_artifact_id bigint,
    resolution_status text DEFAULT 'unresolved'::text NOT NULL,
    first_seen_at timestamp with time zone NOT NULL,
    last_seen_at timestamp with time zone NOT NULL,
    first_seen_run_id uuid NOT NULL,
    last_seen_run_id uuid NOT NULL,
    canonical_detail_url text,
    last_seen_price integer,
    last_seen_dealer text,
    last_seen_distance_miles numeric,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- TOC entry 217 (class 1259 OID 16400)
-- Name: raw_artifacts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.raw_artifacts (
    artifact_id bigint NOT NULL,
    run_id uuid NOT NULL,
    source text NOT NULL,
    artifact_type text NOT NULL,
    search_key text,
    search_scope text,
    url text NOT NULL,
    fetched_at timestamp with time zone DEFAULT now() NOT NULL,
    http_status integer,
    content_type text,
    content_bytes bigint,
    sha256 text,
    filepath text NOT NULL,
    error text,
    page_num integer
);


--
-- TOC entry 216 (class 1259 OID 16399)
-- Name: raw_artifacts_artifact_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.raw_artifacts_artifact_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 3561 (class 0 OID 0)
-- Dependencies: 216
-- Name: raw_artifacts_artifact_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.raw_artifacts_artifact_id_seq OWNED BY public.raw_artifacts.artifact_id;


--
-- TOC entry 215 (class 1259 OID 16389)
-- Name: runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.runs (
    run_id uuid NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    status text DEFAULT 'running'::text NOT NULL,
    trigger text DEFAULT 'schedule'::text NOT NULL,
    notes text
);


--
-- TOC entry 218 (class 1259 OID 16416)
-- Name: search_configs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.search_configs (
    search_key text NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    source text DEFAULT 'cars.com'::text NOT NULL,
    params jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- TOC entry 222 (class 1259 OID 32794)
-- Name: srp_observations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.srp_observations (
    id bigint NOT NULL,
    artifact_id bigint NOT NULL,
    run_id uuid,
    fetched_at timestamp with time zone,
    listing_id text NOT NULL,
    vin text,
    seller_customer_id text,
    price integer,
    msrp integer,
    mileage integer,
    year integer,
    make text,
    model text,
    "trim" text,
    stock_type text,
    fuel_type text,
    body_style text,
    financing_type text,
    seller_zip text,
    page_number integer,
    position_on_page integer,
    trid text,
    isa_context text,
    canonical_detail_url text,
    raw_vehicle_json jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- TOC entry 228 (class 1259 OID 41066)
-- Name: srp_listing_to_vin; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.srp_listing_to_vin AS
 WITH candidates AS (
         SELECT so.listing_id,
            so.vin,
            so.fetched_at AS vin_observed_at,
            so.artifact_id AS vin_artifact_id,
            row_number() OVER (PARTITION BY so.listing_id ORDER BY so.fetched_at DESC, so.artifact_id DESC) AS rn
           FROM public.srp_observations so
          WHERE ((so.vin IS NOT NULL) AND (length(so.vin) = 17))
        )
 SELECT listing_id,
    vin,
    vin_observed_at,
    vin_artifact_id
   FROM candidates
  WHERE (rn = 1);


--
-- TOC entry 221 (class 1259 OID 32793)
-- Name: srp_observations_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.srp_observations_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 3562 (class 0 OID 0)
-- Dependencies: 221
-- Name: srp_observations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.srp_observations_id_seq OWNED BY public.srp_observations.id;


--
-- TOC entry 223 (class 1259 OID 40983)
-- Name: vehicles; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.vehicles (
    vin text NOT NULL,
    current_listing_id text NOT NULL,
    canonical_detail_url text,
    year integer,
    make text,
    model text,
    "trim" text,
    stock_type text,
    fuel_type text,
    body_style text,
    financing_type text,
    price integer,
    msrp integer,
    mileage integer,
    price_is_valid boolean DEFAULT false NOT NULL,
    seller_customer_id text,
    seller_zip text,
    trid text,
    isa_context text,
    raw_vehicle_json jsonb,
    first_seen_at timestamp with time zone NOT NULL,
    last_seen_at timestamp with time zone NOT NULL,
    last_seen_artifact_id bigint NOT NULL,
    last_seen_run_id uuid,
    last_seen_search_key text,
    last_seen_search_scope text,
    last_seen_page_number integer,
    last_seen_position_on_page integer,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    price_last_seen_at timestamp with time zone,
    price_last_seen_source text,
    price_last_artifact_id bigint,
    full_details_last_updated_at timestamp with time zone,
    full_details_last_updated_source text,
    full_details_last_artifact_id bigint,
    CONSTRAINT vehicles_full_details_last_updated_source_chk CHECK (((full_details_last_updated_source IS NULL) OR (full_details_last_updated_source = ANY (ARRAY['srp'::text, 'detail_primary'::text])))),
    CONSTRAINT vehicles_price_last_seen_source_chk CHECK (((price_last_seen_source IS NULL) OR (price_last_seen_source = ANY (ARRAY['srp'::text, 'detail_primary'::text, 'detail_carousel'::text])))),
    CONSTRAINT vehicles_vin_len_17 CHECK ((length(vin) = 17))
);


--
-- TOC entry 231 (class 1259 OID 41477)
-- Name: vin_current_state; Type: MATERIALIZED VIEW; Schema: public; Owner: -
--

CREATE MATERIALIZED VIEW public.vin_current_state AS
 SELECT DISTINCT ON (vin) vin,
    listing_state,
    fetched_at AS state_seen_at,
    artifact_id AS state_artifact_id
   FROM public.detail_observations d
  WHERE ((vin IS NOT NULL) AND (vin <> ''::text))
  ORDER BY vin, fetched_at DESC, id DESC
  WITH NO DATA;


--
-- TOC entry 233 (class 1259 OID 41530)
-- Name: vin_current_state_v; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.vin_current_state_v AS
 SELECT DISTINCT ON (vin) vin,
    listing_state,
    fetched_at AS state_seen_at,
    artifact_id AS state_artifact_id
   FROM public.detail_observations d
  WHERE ((vin IS NOT NULL) AND (vin <> ''::text))
  ORDER BY vin, fetched_at DESC, id DESC;


--
-- TOC entry 229 (class 1259 OID 41071)
-- Name: vin_to_listing_candidates; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.vin_to_listing_candidates AS
 WITH agg AS (
         SELECT so.vin,
            so.listing_id,
            max(so.fetched_at) AS last_seen_at,
            max(so.artifact_id) FILTER (WHERE (so.fetched_at IS NOT NULL)) AS last_seen_artifact_id
           FROM public.srp_observations so
          WHERE ((so.vin IS NOT NULL) AND (length(so.vin) = 17))
          GROUP BY so.vin, so.listing_id
        ), ranked AS (
         SELECT a.vin,
            a.listing_id,
            a.last_seen_at,
            a.last_seen_artifact_id,
            row_number() OVER (PARTITION BY a.vin ORDER BY a.last_seen_at DESC, a.listing_id DESC) AS listing_rank
           FROM agg a
        )
 SELECT vin,
    listing_id,
    last_seen_at,
    last_seen_artifact_id,
    listing_rank
   FROM ranked;


--
-- TOC entry 3347 (class 2604 OID 41052)
-- Name: detail_carousel_hints id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.detail_carousel_hints ALTER COLUMN id SET DEFAULT nextval('public.detail_carousel_hints_id_seq'::regclass);


--
-- TOC entry 3345 (class 2604 OID 41051)
-- Name: detail_observations id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.detail_observations ALTER COLUMN id SET DEFAULT nextval('public.detail_observations_id_seq'::regclass);


--
-- TOC entry 3329 (class 2604 OID 16403)
-- Name: raw_artifacts artifact_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_artifacts ALTER COLUMN artifact_id SET DEFAULT nextval('public.raw_artifacts_artifact_id_seq'::regclass);


--
-- TOC entry 3340 (class 2604 OID 32797)
-- Name: srp_observations id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.srp_observations ALTER COLUMN id SET DEFAULT nextval('public.srp_observations_id_seq'::regclass);


--
-- TOC entry 3358 (class 2606 OID 16434)
-- Name: artifact_processing artifact_processing_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.artifact_processing
    ADD CONSTRAINT artifact_processing_pkey PRIMARY KEY (artifact_id, processor);


--
-- TOC entry 3391 (class 2606 OID 41041)
-- Name: detail_carousel_hints detail_carousel_hints_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.detail_carousel_hints
    ADD CONSTRAINT detail_carousel_hints_pkey PRIMARY KEY (id);


--
-- TOC entry 3385 (class 2606 OID 41023)
-- Name: detail_observations detail_observations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.detail_observations
    ADD CONSTRAINT detail_observations_pkey PRIMARY KEY (id);


--
-- TOC entry 3365 (class 2606 OID 16451)
-- Name: listings listings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.listings
    ADD CONSTRAINT listings_pkey PRIMARY KEY (listing_id);


--
-- TOC entry 3354 (class 2606 OID 16408)
-- Name: raw_artifacts raw_artifacts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_artifacts
    ADD CONSTRAINT raw_artifacts_pkey PRIMARY KEY (artifact_id);


--
-- TOC entry 3352 (class 2606 OID 16398)
-- Name: runs runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_pkey PRIMARY KEY (run_id);


--
-- TOC entry 3356 (class 2606 OID 16426)
-- Name: search_configs search_configs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.search_configs
    ADD CONSTRAINT search_configs_pkey PRIMARY KEY (search_key);


--
-- TOC entry 3373 (class 2606 OID 32804)
-- Name: srp_observations srp_observations_artifact_listing_uq; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.srp_observations
    ADD CONSTRAINT srp_observations_artifact_listing_uq UNIQUE (artifact_id, listing_id);


--
-- TOC entry 3375 (class 2606 OID 32802)
-- Name: srp_observations srp_observations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.srp_observations
    ADD CONSTRAINT srp_observations_pkey PRIMARY KEY (id);


--
-- TOC entry 3395 (class 2606 OID 41043)
-- Name: detail_carousel_hints uq_detail_carousel_hints_artifact_listing; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.detail_carousel_hints
    ADD CONSTRAINT uq_detail_carousel_hints_artifact_listing UNIQUE (artifact_id, listing_id);


--
-- TOC entry 3389 (class 2606 OID 41025)
-- Name: detail_observations uq_detail_observations_artifact_listing; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.detail_observations
    ADD CONSTRAINT uq_detail_observations_artifact_listing UNIQUE (artifact_id, listing_id);


--
-- TOC entry 3382 (class 2606 OID 40993)
-- Name: vehicles vehicles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.vehicles
    ADD CONSTRAINT vehicles_pkey PRIMARY KEY (vin);


--
-- TOC entry 3359 (class 1259 OID 16440)
-- Name: idx_artifact_processing_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_artifact_processing_status ON public.artifact_processing USING btree (processor, status, processed_at);


--
-- TOC entry 3361 (class 1259 OID 16459)
-- Name: idx_listings_last_seen_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_listings_last_seen_at ON public.listings USING btree (last_seen_at);


--
-- TOC entry 3362 (class 1259 OID 16458)
-- Name: idx_listings_resolution_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_listings_resolution_status ON public.listings USING btree (resolution_status);


--
-- TOC entry 3363 (class 1259 OID 16457)
-- Name: idx_listings_vin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_listings_vin ON public.listings USING btree (vin);


--
-- TOC entry 3366 (class 1259 OID 41000)
-- Name: idx_srp_obs_vin_fetched; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_srp_obs_vin_fetched ON public.srp_observations USING btree (vin, fetched_at DESC, artifact_id DESC);


--
-- TOC entry 3376 (class 1259 OID 40994)
-- Name: idx_vehicles_last_seen_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_vehicles_last_seen_at ON public.vehicles USING btree (last_seen_at);


--
-- TOC entry 3377 (class 1259 OID 40995)
-- Name: idx_vehicles_make_model_year; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_vehicles_make_model_year ON public.vehicles USING btree (make, model, year);


--
-- TOC entry 3378 (class 1259 OID 40997)
-- Name: idx_vehicles_price_is_valid; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_vehicles_price_is_valid ON public.vehicles USING btree (price_is_valid);


--
-- TOC entry 3379 (class 1259 OID 40996)
-- Name: idx_vehicles_seller_customer_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_vehicles_seller_customer_id ON public.vehicles USING btree (seller_customer_id);


--
-- TOC entry 3392 (class 1259 OID 41049)
-- Name: ix_detail_carousel_hints_listing_id_fetched_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_detail_carousel_hints_listing_id_fetched_at ON public.detail_carousel_hints USING btree (listing_id, fetched_at DESC);


--
-- TOC entry 3393 (class 1259 OID 41050)
-- Name: ix_detail_carousel_hints_source_listing_id_fetched_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_detail_carousel_hints_source_listing_id_fetched_at ON public.detail_carousel_hints USING btree (source_listing_id, fetched_at DESC);


--
-- TOC entry 3386 (class 1259 OID 41032)
-- Name: ix_detail_observations_listing_id_fetched_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_detail_observations_listing_id_fetched_at ON public.detail_observations USING btree (listing_id, fetched_at DESC);


--
-- TOC entry 3387 (class 1259 OID 41031)
-- Name: ix_detail_observations_vin_fetched_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_detail_observations_vin_fetched_at ON public.detail_observations USING btree (vin, fetched_at DESC);


--
-- TOC entry 3396 (class 1259 OID 41467)
-- Name: listing_current_state_pk; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX listing_current_state_pk ON public.listing_current_state USING btree (listing_id);


--
-- TOC entry 3397 (class 1259 OID 41468)
-- Name: listing_current_state_state_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX listing_current_state_state_idx ON public.listing_current_state USING btree (listing_state);


--
-- TOC entry 3367 (class 1259 OID 32814)
-- Name: srp_obs_artifact_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX srp_obs_artifact_id_idx ON public.srp_observations USING btree (artifact_id);


--
-- TOC entry 3368 (class 1259 OID 32813)
-- Name: srp_obs_fetched_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX srp_obs_fetched_at_idx ON public.srp_observations USING btree (fetched_at);


--
-- TOC entry 3369 (class 1259 OID 32811)
-- Name: srp_obs_listing_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX srp_obs_listing_id_idx ON public.srp_observations USING btree (listing_id);


--
-- TOC entry 3370 (class 1259 OID 32812)
-- Name: srp_obs_seller_customer_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX srp_obs_seller_customer_id_idx ON public.srp_observations USING btree (seller_customer_id);


--
-- TOC entry 3371 (class 1259 OID 32810)
-- Name: srp_obs_vin_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX srp_obs_vin_idx ON public.srp_observations USING btree (vin);


--
-- TOC entry 3360 (class 1259 OID 24631)
-- Name: uq_artifact_processing_artifact_processor; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_artifact_processing_artifact_processor ON public.artifact_processing USING btree (artifact_id, processor);


--
-- TOC entry 3380 (class 1259 OID 41065)
-- Name: vehicles_full_details_last_updated_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX vehicles_full_details_last_updated_at_idx ON public.vehicles USING btree (full_details_last_updated_at);


--
-- TOC entry 3383 (class 1259 OID 41064)
-- Name: vehicles_price_last_seen_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX vehicles_price_last_seen_at_idx ON public.vehicles USING btree (price_last_seen_at);


--
-- TOC entry 3398 (class 1259 OID 41483)
-- Name: vin_current_state_pk; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX vin_current_state_pk ON public.vin_current_state USING btree (vin);


--
-- TOC entry 3400 (class 2606 OID 16435)
-- Name: artifact_processing artifact_processing_artifact_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.artifact_processing
    ADD CONSTRAINT artifact_processing_artifact_id_fkey FOREIGN KEY (artifact_id) REFERENCES public.raw_artifacts(artifact_id);


--
-- TOC entry 3404 (class 2606 OID 41044)
-- Name: detail_carousel_hints detail_carousel_hints_artifact_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.detail_carousel_hints
    ADD CONSTRAINT detail_carousel_hints_artifact_id_fkey FOREIGN KEY (artifact_id) REFERENCES public.raw_artifacts(artifact_id);


--
-- TOC entry 3403 (class 2606 OID 41026)
-- Name: detail_observations detail_observations_artifact_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.detail_observations
    ADD CONSTRAINT detail_observations_artifact_id_fkey FOREIGN KEY (artifact_id) REFERENCES public.raw_artifacts(artifact_id);


--
-- TOC entry 3401 (class 2606 OID 16452)
-- Name: listings listings_resolution_artifact_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.listings
    ADD CONSTRAINT listings_resolution_artifact_id_fkey FOREIGN KEY (resolution_artifact_id) REFERENCES public.raw_artifacts(artifact_id);


--
-- TOC entry 3399 (class 2606 OID 16409)
-- Name: raw_artifacts raw_artifacts_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_artifacts
    ADD CONSTRAINT raw_artifacts_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.runs(run_id) ON DELETE CASCADE;


--
-- TOC entry 3402 (class 2606 OID 32805)
-- Name: srp_observations srp_observations_artifact_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.srp_observations
    ADD CONSTRAINT srp_observations_artifact_id_fkey FOREIGN KEY (artifact_id) REFERENCES public.raw_artifacts(artifact_id) ON DELETE CASCADE;


-- Completed on 2026-01-28 14:19:48 UTC

--
-- PostgreSQL database dump complete
--

\unrestrict EI4S9eTNd5vfIaQiFadHRaRgcHpdKTjHhvdvl5SZ7M8yV5Ei5gpX8VdpMk70EXk

