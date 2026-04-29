-- V035: Drop n8n and runs-era tables.
-- Run after Track 3 ops code is deployed (ops no longer references these tables).

DROP TABLE IF EXISTS public.n8n_executions;
DROP TABLE IF EXISTS public.pipeline_errors;       -- if not already dropped in V034
DROP TABLE IF EXISTS public.runs CASCADE;          -- CASCADE drops FK from scrape_jobs
DROP TABLE IF EXISTS public.scrape_jobs;
DROP TABLE IF EXISTS public.processing_runs;
DROP TABLE IF EXISTS public.dbt_intents;
DROP TABLE IF EXISTS public.dbt_lock;
DROP TABLE IF EXISTS public.dbt_runs;
