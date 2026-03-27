-- Plan 54: dbt_intents table
-- Replaces the hardcoded INTENT_TO_SELECT dict in dbt_runner/app.py.

CREATE TABLE IF NOT EXISTS public.dbt_intents (
    intent_name TEXT PRIMARY KEY,
    select_args TEXT[] NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now() NOT NULL
);

INSERT INTO public.dbt_intents (intent_name, select_args) VALUES
    ('after_srp',    ARRAY['stg_srp_observations+', 'stg_detail_carousel_hints+', 'ops_vehicle_staleness+']),
    ('after_detail', ARRAY['stg_detail_observations+', 'stg_detail_carousel_hints+', 'ops_vehicle_staleness+'])
ON CONFLICT (intent_name) DO NOTHING;
