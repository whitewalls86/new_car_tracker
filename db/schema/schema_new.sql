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

--
-- Name: analytics; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA analytics;


--
-- Name: ops; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA ops;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: int_carousel_hints_filtered; Type: TABLE; Schema: analytics; Owner: -
--

CREATE TABLE analytics.int_carousel_hints_filtered (
    id bigint,
    artifact_id bigint,
    observed_at timestamp with time zone,
    listing_id text,
    price integer,
    is_valid_target boolean
);


--
-- Name: int_listing_to_vin; Type: TABLE; Schema: analytics; Owner: -
--

CREATE TABLE analytics.int_listing_to_vin (
    listing_id text,
    vin text,
    vin_observed_at timestamp with time zone,
    vin_artifact_id bigint
);


--
-- Name: int_carousel_price_events_mapped; Type: VIEW; Schema: analytics; Owner: -
--

CREATE VIEW analytics.int_carousel_price_events_mapped AS
 SELECT m.vin,
    f.listing_id,
    f.artifact_id,
    f.observed_at,
    f.price,
    'detail_carousel'::text AS source,
    2 AS tier
   FROM (analytics.int_carousel_hints_filtered f
     JOIN analytics.int_listing_to_vin m ON ((m.listing_id = f.listing_id)))
  WHERE (f.is_valid_target = true);


--
-- Name: stg_detail_observations; Type: TABLE; Schema: analytics; Owner: -
--

CREATE TABLE analytics.stg_detail_observations (
    id bigint,
    artifact_id bigint,
    fetched_at timestamp with time zone,
    listing_id text,
    vin text,
    vin17 text,
    listing_state text,
    make text,
    model text,
    vehicle_trim text,
    model_year integer,
    price integer,
    mileage integer,
    msrp integer,
    stock_type text,
    fuel_type text,
    body_style text,
    dealer_name text,
    dealer_zip text,
    customer_id text,
    canonical_detail_url text
);


--
-- Name: int_carousel_price_events_unmapped; Type: VIEW; Schema: analytics; Owner: -
--

CREATE VIEW analytics.int_carousel_price_events_unmapped AS
 SELECT f.listing_id,
    f.artifact_id,
    f.observed_at,
    f.price,
    'detail_carousel'::text AS source,
    2 AS tier
   FROM ((analytics.int_carousel_hints_filtered f
     LEFT JOIN analytics.int_listing_to_vin m ON ((m.listing_id = f.listing_id)))
     LEFT JOIN analytics.stg_detail_observations d ON ((f.listing_id = d.listing_id)))
  WHERE ((f.is_valid_target = true) AND (m.listing_id IS NULL) AND (d.vin IS NULL));


--
-- Name: stg_srp_observations; Type: TABLE; Schema: analytics; Owner: -
--

CREATE TABLE analytics.stg_srp_observations (
    id bigint,
    artifact_id bigint,
    run_id uuid,
    fetched_at timestamp with time zone,
    listing_id text,
    vin text,
    vin17 text,
    seller_customer_id text,
    price integer,
    msrp integer,
    mileage integer,
    model_year integer,
    make text,
    model text,
    vehicle_trim text,
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
    created_at timestamp with time zone
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
-- Name: int_dealer_inventory; Type: VIEW; Schema: analytics; Owner: -
--

CREATE VIEW analytics.int_dealer_inventory AS
 WITH active_listings AS (
         SELECT s.vin17 AS vin,
            s.seller_customer_id,
            s.seller_zip,
            s.make,
            s.model,
            row_number() OVER (PARTITION BY s.vin17 ORDER BY s.fetched_at DESC, s.artifact_id DESC) AS rn
           FROM analytics.stg_srp_observations s
          WHERE ((s.vin17 IS NOT NULL) AND (s.seller_customer_id IS NOT NULL) AND (s.fetched_at >= (now() - '3 days'::interval)))
        ), deduped AS (
         SELECT active_listings.vin,
            active_listings.seller_customer_id,
            active_listings.seller_zip,
            active_listings.make,
            active_listings.model,
            active_listings.rn
           FROM active_listings
          WHERE (active_listings.rn = 1)
        )
 SELECT d.seller_customer_id,
    d.seller_zip,
    dlr.name AS dealer_name,
    dlr.city AS dealer_city,
    dlr.state AS dealer_state,
    d.make,
    d.model,
    count(DISTINCT d.vin) AS dealer_inventory_count
   FROM (deduped d
     LEFT JOIN public.dealers dlr ON ((dlr.customer_id = d.seller_customer_id)))
  GROUP BY d.seller_customer_id, d.seller_zip, dlr.name, dlr.city, dlr.state, d.make, d.model;


--
-- Name: int_latest_dealer_name_by_vin; Type: TABLE; Schema: analytics; Owner: -
--

CREATE TABLE analytics.int_latest_dealer_name_by_vin (
    vin text,
    dealer_name text,
    artifact_id bigint
);


--
-- Name: int_latest_price_by_vin; Type: TABLE; Schema: analytics; Owner: -
--

CREATE TABLE analytics.int_latest_price_by_vin (
    vin text,
    price integer,
    price_observed_at timestamp with time zone,
    price_artifact_id bigint,
    price_listing_id text,
    price_source text,
    price_tier integer
);


--
-- Name: int_latest_tier1_observation_by_vin; Type: TABLE; Schema: analytics; Owner: -
--

CREATE TABLE analytics.int_latest_tier1_observation_by_vin (
    vin text,
    listing_id text,
    artifact_id bigint,
    observed_at timestamp with time zone,
    listing_state text,
    mileage integer,
    canonical_detail_url text,
    seller_customer_id text,
    customer_id text,
    source text
);


--
-- Name: int_listing_current_state; Type: TABLE; Schema: analytics; Owner: -
--

CREATE TABLE analytics.int_listing_current_state (
    listing_id text,
    listing_state text,
    listing_state_seen_at timestamp with time zone,
    listing_state_artifact_id bigint
);


--
-- Name: int_listing_days_on_market; Type: TABLE; Schema: analytics; Owner: -
--

CREATE TABLE analytics.int_listing_days_on_market (
    vin text,
    first_seen_at timestamp with time zone,
    last_seen_at timestamp with time zone,
    first_seen_national_at timestamp with time zone,
    first_seen_local_at timestamp with time zone,
    last_seen_local_at timestamp with time zone,
    days_on_market integer,
    days_observed bigint
);


--
-- Name: int_model_price_benchmarks; Type: TABLE; Schema: analytics; Owner: -
--

CREATE TABLE analytics.int_model_price_benchmarks (
    make text,
    model text,
    vehicle_trim text,
    national_listing_count bigint,
    national_avg_price integer,
    national_median_price integer,
    national_p10_price integer,
    national_p25_price integer,
    national_p75_price integer,
    national_p90_price integer,
    national_avg_msrp integer,
    national_avg_discount_pct numeric(5,2)
);


--
-- Name: int_price_events; Type: TABLE; Schema: analytics; Owner: -
--

CREATE TABLE analytics.int_price_events (
    vin text,
    listing_id text,
    artifact_id bigint,
    observed_at timestamp with time zone,
    price integer,
    source text,
    tier integer
);


--
-- Name: int_price_history_by_vin; Type: VIEW; Schema: analytics; Owner: -
--

CREATE VIEW analytics.int_price_history_by_vin AS
 WITH ordered AS (
         SELECT int_price_events.vin,
            int_price_events.observed_at,
            int_price_events.price,
            lag(int_price_events.price) OVER (PARTITION BY int_price_events.vin ORDER BY int_price_events.observed_at, int_price_events.artifact_id) AS prev_price,
            row_number() OVER (PARTITION BY int_price_events.vin ORDER BY int_price_events.observed_at, int_price_events.artifact_id) AS obs_num
           FROM analytics.int_price_events
          WHERE ((int_price_events.price IS NOT NULL) AND (int_price_events.price > 0))
        )
 SELECT vin,
    min(
        CASE
            WHEN (obs_num = 1) THEN price
            ELSE NULL::integer
        END) AS first_price,
    min(
        CASE
            WHEN (obs_num = 1) THEN observed_at
            ELSE NULL::timestamp with time zone
        END) AS first_price_observed_at,
    min(price) AS min_price,
    max(price) AS max_price,
    count(
        CASE
            WHEN ((prev_price IS NOT NULL) AND (price < prev_price)) THEN 1
            ELSE NULL::integer
        END) AS price_drop_count,
    count(
        CASE
            WHEN ((prev_price IS NOT NULL) AND (price > prev_price)) THEN 1
            ELSE NULL::integer
        END) AS price_increase_count,
    count(*) AS total_price_observations
   FROM ordered
  GROUP BY vin;


--
-- Name: int_price_percentiles_by_vin; Type: TABLE; Schema: analytics; Owner: -
--

CREATE TABLE analytics.int_price_percentiles_by_vin (
    vin text,
    national_price_percentile double precision
);


--
-- Name: stg_raw_artifacts; Type: TABLE; Schema: analytics; Owner: -
--

CREATE TABLE analytics.stg_raw_artifacts (
    artifact_id bigint,
    run_id uuid,
    search_key text,
    search_scope text,
    fetched_at timestamp with time zone,
    http_status integer
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
-- Name: stg_search_configs; Type: VIEW; Schema: analytics; Owner: -
--

CREATE VIEW analytics.stg_search_configs AS
 SELECT search_key,
    enabled,
    source,
    rotation_slot,
    last_queued_at,
    (params ->> 'zip'::text) AS zip,
    ((params ->> 'radius_miles'::text))::integer AS radius_miles,
    ((params ->> 'max_listings'::text))::integer AS max_listings,
    ((params ->> 'max_safety_pages'::text))::integer AS max_safety_pages,
    ((params -> 'makes'::text) ->> 0) AS make_slug,
    ((params -> 'models'::text) ->> 0) AS model_slug
   FROM public.search_configs;


--
-- Name: int_scrape_targets; Type: VIEW; Schema: analytics; Owner: -
--

CREATE VIEW analytics.int_scrape_targets AS
 SELECT DISTINCT ON (sc.search_key) sc.search_key,
    sc.enabled,
    sc.make_slug,
    sc.model_slug,
    COALESCE(obs.make, sc.make_slug) AS make,
    COALESCE(obs.model, sc.model_slug) AS model
   FROM ((analytics.stg_search_configs sc
     LEFT JOIN analytics.stg_raw_artifacts ra ON ((ra.search_key = sc.search_key)))
     LEFT JOIN analytics.stg_srp_observations obs ON ((obs.artifact_id = ra.artifact_id)))
  WHERE ((obs.make IS NOT NULL) AND (sc.enabled = true))
  ORDER BY sc.search_key, obs.fetched_at DESC;


--
-- Name: int_vehicle_attributes; Type: TABLE; Schema: analytics; Owner: -
--

CREATE TABLE analytics.int_vehicle_attributes (
    vin text,
    make text,
    model text,
    vehicle_trim text,
    model_year integer,
    msrp integer,
    fuel_type text,
    body_style text,
    stock_type text,
    financing_type text,
    seller_zip text,
    seller_customer_id text,
    canonical_detail_url text,
    search_key text,
    search_scope text,
    attributes_observed_at timestamp with time zone,
    attributes_artifact_id bigint,
    attributes_source text,
    first_seen_at timestamp with time zone,
    last_seen_at timestamp with time zone,
    is_tracked boolean
);


--
-- Name: int_vin_current_state; Type: TABLE; Schema: analytics; Owner: -
--

CREATE TABLE analytics.int_vin_current_state (
    vin text,
    listing_state text,
    state_seen_at timestamp with time zone,
    state_artifact_id bigint
);


--
-- Name: mart_deal_scores; Type: TABLE; Schema: analytics; Owner: -
--

CREATE TABLE analytics.mart_deal_scores (
    vin text,
    listing_id text,
    make text,
    model text,
    vehicle_trim text,
    model_year integer,
    fuel_type text,
    body_style text,
    stock_type text,
    search_key text,
    canonical_detail_url text,
    seller_customer_id text,
    seller_zip text,
    dealer_name text,
    dealer_city text,
    dealer_state text,
    dealer_phone text,
    dealer_rating numeric(3,1),
    current_price integer,
    price_observed_at timestamp with time zone,
    price_source text,
    msrp integer,
    msrp_discount_pct numeric,
    msrp_discount_amt integer,
    first_seen_at timestamp with time zone,
    last_seen_at timestamp with time zone,
    first_seen_local_at timestamp with time zone,
    days_on_market integer,
    days_observed bigint,
    first_price integer,
    min_price integer,
    max_price integer,
    price_drop_count bigint,
    price_increase_count bigint,
    total_price_observations bigint,
    total_price_drop_pct numeric,
    national_listing_count bigint,
    national_avg_price integer,
    national_median_price integer,
    national_p10_price integer,
    national_p25_price integer,
    national_avg_discount_pct numeric(5,2),
    national_price_percentile double precision,
    dealer_inventory_count bigint,
    is_local boolean,
    listing_state text,
    deal_score numeric,
    deal_tier text
);


--
-- Name: mart_vehicle_snapshot; Type: TABLE; Schema: analytics; Owner: -
--

CREATE TABLE analytics.mart_vehicle_snapshot (
    vin text,
    listing_id text,
    listing_state text,
    mileage integer,
    tier1_observed_at timestamp with time zone,
    tier1_artifact_id bigint,
    tier1_source text,
    tier1_canonical_detail_url text,
    tier1_seller_customer_id text,
    customer_id text,
    current_listing_url text,
    price integer,
    price_observed_at timestamp with time zone,
    price_artifact_id bigint,
    price_source text,
    price_tier integer
);


--
-- Name: scrape_targets; Type: TABLE; Schema: analytics; Owner: -
--

CREATE TABLE analytics.scrape_targets (
    search_key text,
    make text,
    model text
);


--
-- Name: blocked_cooldown; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.blocked_cooldown (
    listing_id text NOT NULL,
    first_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    last_attempted_at timestamp with time zone DEFAULT now() NOT NULL,
    num_of_attempts integer DEFAULT 1 NOT NULL
);


--
-- Name: stg_blocked_cooldown; Type: VIEW; Schema: analytics; Owner: -
--

CREATE VIEW analytics.stg_blocked_cooldown AS
 SELECT listing_id,
    first_attempt_at,
    last_attempted_at,
    num_of_attempts,
        CASE
            WHEN (num_of_attempts >= 5) THEN NULL::timestamp with time zone
            ELSE (last_attempted_at + ('01:00:00'::interval * ((12)::double precision * power((2)::double precision, ((num_of_attempts)::double precision - (1)::double precision)))))
        END AS next_eligible_at,
    (num_of_attempts >= 5) AS fully_blocked
   FROM public.blocked_cooldown;


--
-- Name: stg_dealers; Type: VIEW; Schema: analytics; Owner: -
--

CREATE VIEW analytics.stg_dealers AS
 SELECT customer_id,
    name,
    city,
    state,
    zip,
    phone,
    rating
   FROM public.dealers
  WHERE (length(customer_id) < 36);


--
-- Name: stg_detail_carousel_hints; Type: TABLE; Schema: analytics; Owner: -
--

CREATE TABLE analytics.stg_detail_carousel_hints (
    id bigint,
    artifact_id bigint,
    fetched_at timestamp with time zone,
    source_listing_id text,
    listing_id text,
    price integer,
    mileage integer,
    body text,
    condition text,
    year integer
);


--
-- Name: ops_vehicle_staleness; Type: VIEW; Schema: ops; Owner: -
--

CREATE VIEW ops.ops_vehicle_staleness AS
 WITH base AS (
         SELECT mart_vehicle_snapshot.vin,
            mart_vehicle_snapshot.listing_id,
            mart_vehicle_snapshot.tier1_observed_at,
            mart_vehicle_snapshot.tier1_artifact_id,
            mart_vehicle_snapshot.tier1_source,
            mart_vehicle_snapshot.current_listing_url,
            mart_vehicle_snapshot.tier1_seller_customer_id,
            mart_vehicle_snapshot.customer_id,
            mart_vehicle_snapshot.listing_state,
            mart_vehicle_snapshot.price,
            mart_vehicle_snapshot.price_observed_at,
            mart_vehicle_snapshot.price_artifact_id,
            mart_vehicle_snapshot.price_source,
            mart_vehicle_snapshot.price_tier
           FROM analytics.mart_vehicle_snapshot
          WHERE (mart_vehicle_snapshot.listing_state <> 'unlisted'::text)
        ), computed AS (
         SELECT b.vin,
            b.listing_id,
            b.tier1_observed_at,
            b.tier1_artifact_id,
            b.tier1_source,
            b.current_listing_url,
            b.tier1_seller_customer_id,
            b.customer_id,
            b.listing_state,
            b.price,
            b.price_observed_at,
            b.price_artifact_id,
            b.price_source,
            b.price_tier,
            (now() - b.tier1_observed_at) AS tier1_age_interval,
            (now() - b.price_observed_at) AS price_age_interval,
            (EXTRACT(epoch FROM (now() - b.tier1_observed_at)) / 3600.0) AS tier1_age_hours,
                CASE
                    WHEN (b.price_observed_at IS NULL) THEN NULL::numeric
                    ELSE (EXTRACT(epoch FROM (now() - b.price_observed_at)) / 3600.0)
                END AS price_age_hours,
            ((b.customer_id IS NULL) AND (b.price_source <> 'detail'::text)) AS dealer_unenriched
           FROM base b
        ), flags AS (
         SELECT c.vin,
            c.listing_id,
            c.tier1_observed_at,
            c.tier1_artifact_id,
            c.tier1_source,
            c.current_listing_url,
            c.tier1_seller_customer_id,
            c.customer_id,
            c.listing_state,
            c.price,
            c.price_observed_at,
            c.price_artifact_id,
            c.price_source,
            c.price_tier,
            c.tier1_age_interval,
            c.price_age_interval,
            c.tier1_age_hours,
            c.price_age_hours,
            c.dealer_unenriched,
            ((c.tier1_age_hours > 168.0) OR c.dealer_unenriched) AS is_full_details_stale,
            ((c.price_observed_at IS NULL) OR (c.price_age_hours > 24.0)) AS is_price_stale,
                CASE
                    WHEN (c.dealer_unenriched AND (c.listing_state = 'active'::text)) THEN 'dealer_unenriched'::text
                    WHEN (c.tier1_age_hours > 168.0) THEN 'full_details'::text
                    WHEN ((c.price_observed_at IS NULL) OR (c.price_age_hours > 24.0)) THEN 'price_only'::text
                    ELSE 'not_stale'::text
                END AS stale_reason
           FROM computed c
        )
 SELECT vin,
    listing_id,
    tier1_observed_at,
    tier1_artifact_id,
    tier1_source,
    price,
    price_observed_at,
    price_artifact_id,
    price_source,
    price_tier,
    tier1_age_interval,
    price_age_interval,
    tier1_age_hours,
    price_age_hours,
    listing_state,
    current_listing_url,
    tier1_seller_customer_id,
    customer_id,
    is_full_details_stale,
    is_price_stale,
    stale_reason
   FROM flags
  WHERE (listing_state IS DISTINCT FROM 'unlisted'::text);


--
-- Name: ops_detail_scrape_queue; Type: VIEW; Schema: ops; Owner: -
--

CREATE VIEW ops.ops_detail_scrape_queue AS
 WITH stale AS (
         SELECT ovs.vin,
            ovs.current_listing_url,
            ovs.listing_id,
            COALESCE(ovs.tier1_seller_customer_id, ovs.customer_id) AS seller_customer_id,
            ovs.is_price_stale,
            ovs.is_full_details_stale,
            ovs.stale_reason,
            ovs.tier1_age_hours,
            ovs.price_age_hours,
            row_number() OVER (PARTITION BY COALESCE(ovs.tier1_seller_customer_id, ovs.vin) ORDER BY
                CASE
                    WHEN ovs.is_full_details_stale THEN 0
                    ELSE 1
                END, COALESCE(ovs.price_observed_at, '1970-01-01 00:00:00+00'::timestamp with time zone), COALESCE(ovs.tier1_observed_at, '1970-01-01 00:00:00+00'::timestamp with time zone)) AS dealer_row_num
           FROM ops.ops_vehicle_staleness ovs
          WHERE ((ovs.is_price_stale OR ovs.is_full_details_stale) AND (COALESCE(ovs.listing_state, 'active'::text) = 'active'::text) AND (ovs.current_listing_url IS NOT NULL))
        ), dealer_picks AS (
         SELECT stale.vin,
            stale.current_listing_url,
            stale.listing_id,
            stale.seller_customer_id,
            stale.stale_reason,
            1 AS priority
           FROM stale
          WHERE (stale.dealer_row_num = 1)
        ), force_stale AS (
         SELECT stale.vin,
            stale.current_listing_url,
            stale.listing_id,
            stale.seller_customer_id,
            'force_stale_36h'::text AS stale_reason,
            2 AS priority
           FROM stale
          WHERE ((stale.price_age_hours > (36)::numeric) AND (stale.dealer_row_num > 1))
        ), carousel AS (
         SELECT sub.listing_id AS vin,
            (('https://www.cars.com/vehicledetail/'::text || sub.listing_id) || '/'::text) AS current_listing_url,
            sub.listing_id,
            NULL::text AS seller_customer_id,
            'unmapped_carousel'::text AS stale_reason,
            3 AS priority
           FROM ( SELECT int_carousel_price_events_unmapped.listing_id,
                    row_number() OVER (PARTITION BY int_carousel_price_events_unmapped.listing_id ORDER BY int_carousel_price_events_unmapped.observed_at DESC) AS rn
                   FROM analytics.int_carousel_price_events_unmapped) sub
          WHERE (sub.rn = 1)
        ), capacity_fill AS (
         SELECT stale.vin,
            stale.current_listing_url,
            stale.listing_id,
            stale.seller_customer_id,
            concat(stale.stale_reason, '-extra') AS stale_reason,
            4 AS priority
           FROM stale
          WHERE (stale.dealer_row_num > 1)
        ), combined AS (
         SELECT dealer_picks.vin,
            dealer_picks.current_listing_url,
            dealer_picks.listing_id,
            dealer_picks.seller_customer_id,
            dealer_picks.stale_reason,
            dealer_picks.priority
           FROM dealer_picks
        UNION ALL
         SELECT force_stale.vin,
            force_stale.current_listing_url,
            force_stale.listing_id,
            force_stale.seller_customer_id,
            force_stale.stale_reason,
            force_stale.priority
           FROM force_stale
        UNION ALL
         SELECT carousel.vin,
            carousel.current_listing_url,
            carousel.listing_id,
            carousel.seller_customer_id,
            carousel.stale_reason,
            carousel.priority
           FROM carousel
        UNION ALL
         SELECT capacity_fill.vin,
            capacity_fill.current_listing_url,
            capacity_fill.listing_id,
            capacity_fill.seller_customer_id,
            capacity_fill.stale_reason,
            capacity_fill.priority
           FROM capacity_fill
        )
 SELECT DISTINCT ON (c.listing_id) c.vin,
    c.current_listing_url,
    c.listing_id,
    c.seller_customer_id,
    c.stale_reason,
    c.priority
   FROM (combined c
     LEFT JOIN analytics.stg_blocked_cooldown bc ON ((bc.listing_id = c.listing_id)))
  WHERE ((bc.listing_id IS NULL) OR ((bc.fully_blocked = false) AND (bc.next_eligible_at < now())))
  ORDER BY c.listing_id, c.priority;


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
-- Name: dbt_intents; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dbt_intents (
    intent_name text NOT NULL,
    select_args text[] NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: dbt_lock; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dbt_lock (
    id integer DEFAULT 1 NOT NULL,
    locked boolean DEFAULT false NOT NULL,
    locked_at timestamp with time zone,
    locked_by text,
    CONSTRAINT single_row CHECK ((id = 1))
);


--
-- Name: dbt_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dbt_runs (
    id integer NOT NULL,
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


--
-- Name: dbt_runs_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.dbt_runs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dbt_runs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.dbt_runs_id_seq OWNED BY public.dbt_runs.id;


--
-- Name: deploy_intent; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.deploy_intent (
    id integer DEFAULT 1 NOT NULL,
    intent text DEFAULT 'none'::text NOT NULL,
    requested_at timestamp with time zone,
    requested_by text,
    completed_at timestamp with time zone
);


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
    dealer_zip text,
    customer_id text,
    make text,
    model text,
    "trim" text,
    year integer,
    fuel_type text,
    body_style text
);


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
-- Name: detail_scrape_claims; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.detail_scrape_claims (
    listing_id text NOT NULL,
    claimed_by text NOT NULL,
    claimed_at timestamp with time zone DEFAULT now() NOT NULL,
    status text DEFAULT 'running'::text NOT NULL
);


--
-- Name: n8n_executions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.n8n_executions (
    execution_id text NOT NULL,
    workflow_name text NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    status text DEFAULT 'running'::text NOT NULL
);


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
-- Name: TABLE pipeline_errors; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.pipeline_errors IS 'Logs errors from n8n workflow executions for pipeline health monitoring';


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
-- Name: processing_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.processing_runs (
    run_id uuid NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    status text DEFAULT 'processing'::text NOT NULL,
    notes text,
    progress_count integer DEFAULT 0,
    total_count integer,
    error_count integer DEFAULT 0,
    last_error text
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
    deleted_at timestamp with time zone,
    listing_id uuid
);


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
    total_count integer,
    error_count integer DEFAULT 0,
    last_error text
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
-- Name: dbt_runs id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dbt_runs ALTER COLUMN id SET DEFAULT nextval('public.dbt_runs_id_seq'::regclass);


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
-- Name: blocked_cooldown blocked_cooldown_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.blocked_cooldown
    ADD CONSTRAINT blocked_cooldown_pkey PRIMARY KEY (listing_id);


--
-- Name: dbt_intents dbt_intents_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dbt_intents
    ADD CONSTRAINT dbt_intents_pkey PRIMARY KEY (intent_name);


--
-- Name: dbt_lock dbt_lock_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dbt_lock
    ADD CONSTRAINT dbt_lock_pkey PRIMARY KEY (id);


--
-- Name: dbt_runs dbt_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dbt_runs
    ADD CONSTRAINT dbt_runs_pkey PRIMARY KEY (id);


--
-- Name: dealers dealers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dealers
    ADD CONSTRAINT dealers_pkey PRIMARY KEY (customer_id);


--
-- Name: deploy_intent deploy_intent_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.deploy_intent
    ADD CONSTRAINT deploy_intent_pkey PRIMARY KEY (id);


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
-- Name: detail_scrape_claims detail_scrape_claims_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.detail_scrape_claims
    ADD CONSTRAINT detail_scrape_claims_pkey PRIMARY KEY (listing_id);


--
-- Name: n8n_executions n8n_executions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.n8n_executions
    ADD CONSTRAINT n8n_executions_pkey PRIMARY KEY (execution_id);


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
-- Name: ix_detail_obs_unlisted; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_detail_obs_unlisted ON public.detail_observations USING btree (fetched_at DESC) WHERE ((listing_state = 'unlisted'::text) AND (vin IS NOT NULL));


--
-- Name: ix_detail_observations_customer_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_detail_observations_customer_id ON public.detail_observations USING btree (customer_id) WHERE (customer_id IS NOT NULL);


--
-- Name: ix_detail_observations_listing_id_fetched_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_detail_observations_listing_id_fetched_at ON public.detail_observations USING btree (listing_id, fetched_at DESC);


--
-- Name: ix_detail_observations_make_model_fetched; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_detail_observations_make_model_fetched ON public.detail_observations USING btree (make, model, fetched_at DESC) WHERE ((make IS NOT NULL) AND (vin IS NOT NULL));


--
-- Name: ix_detail_observations_vin_fetched_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_detail_observations_vin_fetched_at ON public.detail_observations USING btree (vin, fetched_at DESC);


--
-- Name: raw_artifacts_listing_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX raw_artifacts_listing_id_idx ON public.raw_artifacts USING btree (listing_id) WHERE (artifact_type = 'detail_page'::text);


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

\unrestrict 1Bzv16sDXZ44fODk4OWGdbl7xd2xbouydyNGvbCXZCQcwr5E9kAqcqTkcW3AdhH