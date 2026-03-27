-- Default dbt build intents.
-- Mirrors the fallback values in dbt_runner/app.py (_INTENT_FALLBACK).

INSERT INTO public.dbt_intents (intent_name, select_args) VALUES
    ('after_srp',    ARRAY['stg_srp_observations+', 'stg_detail_carousel_hints+', 'ops_vehicle_staleness+']),
    ('after_detail', ARRAY['stg_detail_observations+', 'stg_detail_carousel_hints+', 'ops_vehicle_staleness+'])
ON CONFLICT (intent_name) DO NOTHING;
