--
-- PostgreSQL database dump
--

-- Dumped from database version 16.11 (Debian 16.11-1.pgdg13+1)
-- Dumped by pg_dump version 16.11 (Debian 16.11-1.pgdg13+1)

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

ALTER TABLE IF EXISTS ONLY public.srp_observations DROP CONSTRAINT IF EXISTS srp_observations_artifact_id_fkey;
ALTER TABLE IF EXISTS ONLY public.scrape_jobs DROP CONSTRAINT IF EXISTS scrape_jobs_run_id_fkey;
ALTER TABLE IF EXISTS ONLY public.raw_artifacts DROP CONSTRAINT IF EXISTS raw_artifacts_run_id_fkey;
ALTER TABLE IF EXISTS ONLY public.detail_observations DROP CONSTRAINT IF EXISTS detail_observations_artifact_id_fkey;
ALTER TABLE IF EXISTS ONLY public.detail_carousel_hints DROP CONSTRAINT IF EXISTS detail_carousel_hints_artifact_id_fkey;
ALTER TABLE IF EXISTS ONLY public.artifact_processing DROP CONSTRAINT IF EXISTS artifact_processing_artifact_id_fkey;
DROP INDEX IF EXISTS public.uq_artifact_processing_artifact_processor;
DROP INDEX IF EXISTS public.srp_obs_vin_idx;
DROP INDEX IF EXISTS public.srp_obs_seller_customer_id_idx;
DROP INDEX IF EXISTS public.srp_obs_listing_id_idx;
DROP INDEX IF EXISTS public.srp_obs_fetched_at_idx;
DROP INDEX IF EXISTS public.srp_obs_artifact_id_idx;
DROP INDEX IF EXISTS public.ix_detail_observations_vin_fetched_at;
DROP INDEX IF EXISTS public.ix_detail_observations_listing_id_fetched_at;
DROP INDEX IF EXISTS public.ix_detail_carousel_hints_source_listing_id_fetched_at;
DROP INDEX IF EXISTS public.ix_detail_carousel_hints_listing_id_fetched_at;
DROP INDEX IF EXISTS public.idx_srp_obs_vin_fetched;
DROP INDEX IF EXISTS public.idx_scrape_jobs_status;
DROP INDEX IF EXISTS public.idx_scrape_jobs_run_id;
DROP INDEX IF EXISTS public.idx_artifact_processing_status;
ALTER TABLE IF EXISTS ONLY public.detail_observations DROP CONSTRAINT IF EXISTS uq_detail_observations_artifact_listing;
ALTER TABLE IF EXISTS ONLY public.detail_carousel_hints DROP CONSTRAINT IF EXISTS uq_detail_carousel_hints_artifact_listing;
ALTER TABLE IF EXISTS ONLY public.srp_observations DROP CONSTRAINT IF EXISTS srp_observations_pkey;
ALTER TABLE IF EXISTS ONLY public.srp_observations DROP CONSTRAINT IF EXISTS srp_observations_artifact_listing_uq;
ALTER TABLE IF EXISTS ONLY public.search_configs DROP CONSTRAINT IF EXISTS search_configs_pkey;
ALTER TABLE IF EXISTS ONLY public.scrape_jobs DROP CONSTRAINT IF EXISTS scrape_jobs_pkey;
ALTER TABLE IF EXISTS ONLY public.runs DROP CONSTRAINT IF EXISTS runs_pkey;
ALTER TABLE IF EXISTS ONLY public.raw_artifacts DROP CONSTRAINT IF EXISTS raw_artifacts_pkey;
ALTER TABLE IF EXISTS ONLY public.pipeline_errors DROP CONSTRAINT IF EXISTS pipeline_errors_pkey;
ALTER TABLE IF EXISTS ONLY public.detail_observations DROP CONSTRAINT IF EXISTS detail_observations_pkey;
ALTER TABLE IF EXISTS ONLY public.detail_carousel_hints DROP CONSTRAINT IF EXISTS detail_carousel_hints_pkey;
ALTER TABLE IF EXISTS ONLY public.dealers DROP CONSTRAINT IF EXISTS dealers_pkey;
ALTER TABLE IF EXISTS ONLY public.artifact_processing DROP CONSTRAINT IF EXISTS artifact_processing_pkey;
ALTER TABLE IF EXISTS public.srp_observations ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.raw_artifacts ALTER COLUMN artifact_id DROP DEFAULT;
ALTER TABLE IF EXISTS public.pipeline_errors ALTER COLUMN error_id DROP DEFAULT;
ALTER TABLE IF EXISTS public.detail_observations ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.detail_carousel_hints ALTER COLUMN id DROP DEFAULT;
DROP SEQUENCE IF EXISTS public.srp_observations_id_seq;
DROP TABLE IF EXISTS public.search_configs;
DROP TABLE IF EXISTS public.scrape_jobs;
DROP TABLE IF EXISTS public.runs;
DROP SEQUENCE IF EXISTS public.raw_artifacts_artifact_id_seq;
DROP SEQUENCE IF EXISTS public.pipeline_errors_error_id_seq;
DROP TABLE IF EXISTS public.pipeline_errors;
DROP SEQUENCE IF EXISTS public.detail_observations_id_seq;
DROP SEQUENCE IF EXISTS public.detail_carousel_hints_id_seq;
DROP TABLE IF EXISTS public.artifact_processing;
DROP TABLE IF EXISTS public.detail_observations;
DROP TABLE IF EXISTS public.raw_artifacts;
DROP TABLE IF EXISTS public.dealers;
DROP TABLE IF EXISTS public.srp_observations;
DROP TABLE IF EXISTS public.detail_carousel_hints;
-- public schema already exists by default


SET default_tablespace = '';

SET default_table_access_method = heap;

--
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
-- Name: dealers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dealers (
    customer_id text NOT NULL,
    name text,
    street text,
    city text,
    state text,
    zip text,
    phone text,
    website text,
    cars_com_url text,
    rating numeric(3,1),
    seller_type text,
    first_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    last_updated_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
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
    page_num integer,
    deleted_at timestamp with time zone
);


--
-- Name: detail_observations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.detail_observations (
    id bigint NOT NULL,
    artifact_id bigint NOT NULL,
    fetched_at timestamp with time zone NOT NULL,
    listing_id text,
    vin text,
    listing_state text DEFAULT 'active'::text NOT NULL,
    make text,
    model text,
    "trim" text,
    year integer,
    price integer,
    mileage integer,
    msrp integer,
    stock_type text,
    fuel_type text,
    body_style text,
    dealer_name text,
    dealer_zip text,
    customer_id text
);


--
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
-- Name: detail_carousel_hints_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.detail_carousel_hints_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: detail_carousel_hints_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.detail_carousel_hints_id_seq OWNED BY public.detail_carousel_hints.id;


--
-- Name: detail_observations_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.detail_observations_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: detail_observations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.detail_observations_id_seq OWNED BY public.detail_observations.id;


--
-- Name: pipeline_errors; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.pipeline_errors (
    error_id integer NOT NULL,
    workflow_name text NOT NULL,
    workflow_id text,
    execution_id text,
    node_name text,
    error_message text,
    error_type text,
    occurred_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: pipeline_errors_error_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.pipeline_errors_error_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: pipeline_errors_error_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.pipeline_errors_error_id_seq OWNED BY public.pipeline_errors.error_id;


--
-- Name: raw_artifacts_artifact_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.raw_artifacts_artifact_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: raw_artifacts_artifact_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.raw_artifacts_artifact_id_seq OWNED BY public.raw_artifacts.artifact_id;


--
-- Name: runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.runs (
    run_id uuid NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    status text DEFAULT 'running'::text NOT NULL,
    trigger text DEFAULT 'schedule'::text NOT NULL,
    notes text,
    progress_count integer DEFAULT 0,
    total_count integer
);


--
-- Name: scrape_jobs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.scrape_jobs (
    job_id uuid DEFAULT gen_random_uuid() NOT NULL,
    run_id uuid NOT NULL,
    search_key text NOT NULL,
    scope text NOT NULL,
    status text DEFAULT 'queued'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    fetched_at timestamp with time zone,
    artifact_count integer,
    error text,
    retry_count integer DEFAULT 0
);


--
-- Name: search_configs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.search_configs (
    search_key text NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    source text DEFAULT 'cars.com'::text NOT NULL,
    params jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    rotation_order integer,
    last_queued_at timestamp with time zone,
    rotation_slot integer
);


--
-- Name: srp_observations_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.srp_observations_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: srp_observations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.srp_observations_id_seq OWNED BY public.srp_observations.id;


--
-- Name: detail_carousel_hints id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.detail_carousel_hints ALTER COLUMN id SET DEFAULT nextval('public.detail_carousel_hints_id_seq'::regclass);


--
-- Name: detail_observations id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.detail_observations ALTER COLUMN id SET DEFAULT nextval('public.detail_observations_id_seq'::regclass);


--
-- Name: pipeline_errors error_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pipeline_errors ALTER COLUMN error_id SET DEFAULT nextval('public.pipeline_errors_error_id_seq'::regclass);


--
-- Name: raw_artifacts artifact_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_artifacts ALTER COLUMN artifact_id SET DEFAULT nextval('public.raw_artifacts_artifact_id_seq'::regclass);


--
-- Name: srp_observations id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.srp_observations ALTER COLUMN id SET DEFAULT nextval('public.srp_observations_id_seq'::regclass);


--
-- Name: artifact_processing artifact_processing_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.artifact_processing
    ADD CONSTRAINT artifact_processing_pkey PRIMARY KEY (artifact_id, processor);


--
-- Name: dealers dealers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dealers
    ADD CONSTRAINT dealers_pkey PRIMARY KEY (customer_id);


--
-- Name: detail_carousel_hints detail_carousel_hints_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.detail_carousel_hints
    ADD CONSTRAINT detail_carousel_hints_pkey PRIMARY KEY (id);


--
-- Name: detail_observations detail_observations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.detail_observations
    ADD CONSTRAINT detail_observations_pkey PRIMARY KEY (id);


--
-- Name: pipeline_errors pipeline_errors_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pipeline_errors
    ADD CONSTRAINT pipeline_errors_pkey PRIMARY KEY (error_id);


--
-- Name: raw_artifacts raw_artifacts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_artifacts
    ADD CONSTRAINT raw_artifacts_pkey PRIMARY KEY (artifact_id);


--
-- Name: runs runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_pkey PRIMARY KEY (run_id);


--
-- Name: scrape_jobs scrape_jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scrape_jobs
    ADD CONSTRAINT scrape_jobs_pkey PRIMARY KEY (job_id);


--
-- Name: search_configs search_configs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.search_configs
    ADD CONSTRAINT search_configs_pkey PRIMARY KEY (search_key);


--
-- Name: srp_observations srp_observations_artifact_listing_uq; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.srp_observations
    ADD CONSTRAINT srp_observations_artifact_listing_uq UNIQUE (artifact_id, listing_id);


--
-- Name: srp_observations srp_observations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.srp_observations
    ADD CONSTRAINT srp_observations_pkey PRIMARY KEY (id);


--
-- Name: detail_carousel_hints uq_detail_carousel_hints_artifact_listing; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.detail_carousel_hints
    ADD CONSTRAINT uq_detail_carousel_hints_artifact_listing UNIQUE (artifact_id, listing_id);


--
-- Name: detail_observations uq_detail_observations_artifact_listing; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.detail_observations
    ADD CONSTRAINT uq_detail_observations_artifact_listing UNIQUE (artifact_id, listing_id);


--
-- Name: idx_artifact_processing_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_artifact_processing_status ON public.artifact_processing USING btree (processor, status, processed_at);


--
-- Name: idx_scrape_jobs_run_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_scrape_jobs_run_id ON public.scrape_jobs USING btree (run_id);


--
-- Name: idx_scrape_jobs_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_scrape_jobs_status ON public.scrape_jobs USING btree (status);


--
-- Name: idx_srp_obs_vin_fetched; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_srp_obs_vin_fetched ON public.srp_observations USING btree (vin, fetched_at DESC, artifact_id DESC);


--
-- Name: ix_detail_carousel_hints_listing_id_fetched_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_detail_carousel_hints_listing_id_fetched_at ON public.detail_carousel_hints USING btree (listing_id, fetched_at DESC);


--
-- Name: ix_detail_carousel_hints_source_listing_id_fetched_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_detail_carousel_hints_source_listing_id_fetched_at ON public.detail_carousel_hints USING btree (source_listing_id, fetched_at DESC);


--
-- Name: ix_detail_observations_listing_id_fetched_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_detail_observations_listing_id_fetched_at ON public.detail_observations USING btree (listing_id, fetched_at DESC);


--
-- Name: ix_detail_observations_vin_fetched_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_detail_observations_vin_fetched_at ON public.detail_observations USING btree (vin, fetched_at DESC);


--
-- Name: srp_obs_artifact_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX srp_obs_artifact_id_idx ON public.srp_observations USING btree (artifact_id);


--
-- Name: srp_obs_fetched_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX srp_obs_fetched_at_idx ON public.srp_observations USING btree (fetched_at);


--
-- Name: srp_obs_listing_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX srp_obs_listing_id_idx ON public.srp_observations USING btree (listing_id);


--
-- Name: srp_obs_seller_customer_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX srp_obs_seller_customer_id_idx ON public.srp_observations USING btree (seller_customer_id);


--
-- Name: srp_obs_vin_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX srp_obs_vin_idx ON public.srp_observations USING btree (vin);


--
-- Name: uq_artifact_processing_artifact_processor; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_artifact_processing_artifact_processor ON public.artifact_processing USING btree (artifact_id, processor);


--
-- Name: artifact_processing artifact_processing_artifact_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.artifact_processing
    ADD CONSTRAINT artifact_processing_artifact_id_fkey FOREIGN KEY (artifact_id) REFERENCES public.raw_artifacts(artifact_id);


--
-- Name: detail_carousel_hints detail_carousel_hints_artifact_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.detail_carousel_hints
    ADD CONSTRAINT detail_carousel_hints_artifact_id_fkey FOREIGN KEY (artifact_id) REFERENCES public.raw_artifacts(artifact_id);


--
-- Name: detail_observations detail_observations_artifact_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.detail_observations
    ADD CONSTRAINT detail_observations_artifact_id_fkey FOREIGN KEY (artifact_id) REFERENCES public.raw_artifacts(artifact_id);


--
-- Name: raw_artifacts raw_artifacts_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_artifacts
    ADD CONSTRAINT raw_artifacts_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.runs(run_id) ON DELETE CASCADE;


--
-- Name: scrape_jobs scrape_jobs_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scrape_jobs
    ADD CONSTRAINT scrape_jobs_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.runs(run_id);


--
-- Name: srp_observations srp_observations_artifact_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.srp_observations
    ADD CONSTRAINT srp_observations_artifact_id_fkey FOREIGN KEY (artifact_id) REFERENCES public.raw_artifacts(artifact_id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

--
-- Name: dbt_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dbt_runs (
    id serial PRIMARY KEY,
    started_at timestamp with time zone NOT NULL,
    finished_at timestamp with time zone NOT NULL,
    duration_s numeric(8,2),
    ok boolean NOT NULL,
    intent text,
    select_args text,
    models_pass integer,
    models_error integer,
    models_skip integer,
    returncode integer
);


-- Create schemas for dbt output
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS ops;

