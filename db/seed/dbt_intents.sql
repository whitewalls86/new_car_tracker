-- Default dbt build intents.
-- Mirrors the fallback values in dbt_runner/app.py (_INTENT_FALLBACK).

INSERT INTO public.dbt_intents (intent_name, select_args) VALUES
    ('after_srp',    ARRAY['stg_raw_artifacts+', 'stg_srp_observations+', 'stg_detail_carousel_hints+']),
    ('after_detail', ARRAY['stg_raw_artifacts+', 'stg_detail_observations+', 'stg_detail_carousel_hints+']),
    ('both', ARRAY['stg_raw_artifacts+', 'stg_srp_observations+', 'stg_detail_observations+', 'stg_detail_carousel_hints+']),
    ('after_403', ARRAY['stg_blocked_cooldown+'])
ON CONFLICT (intent_name) DO NOTHING;
