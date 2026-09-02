-- Plan 162 Stage 6c: an active coordination record may name no surface.
--
-- V043 required a non-empty scope on every record whose phase is not 'none'.
-- ops/coordination_contract.py maps `dashboard` and `pgadmin` to no surfaces at
-- all: both are read-only, losing either admits no production mutation, and the
-- contract records that in `no_pause_reason`. A deploy naming one of them alone
-- therefore wrote targets=["dashboard"], scope=[] and was refused. Both halves
-- were authored by Plan 142 and nothing composed them, so the defect first
-- appeared on a production deploy as 503 "Database unavailable" against a
-- Postgres that was healthy throughout.
--
-- `targets` stays non-empty. That is the clause carrying the invariant worth
-- keeping -- an active record must name what it is coordinating -- and an empty
-- `scope` beside a named target is a true statement rather than a missing one:
-- this coordination pauses nothing. The readers already agree. In
-- ops/coordination_drain.py `required_drain_sources(frozenset())` is empty, so
-- every source reports not_applicable and the record drains immediately; in
-- airflow/dags/sensors.py `cs.scope ?| %s::text[]` is false against an empty
-- array, so no DAG blocks on it.
--
-- The pair is asserted from now on against this constraint rather than against
-- a restatement of it in Python:
-- tests/integration/ops/test_deploy_intent.py enumerates SERVICE_CONTRACTS and
-- requests a lone deploy of every service in it.

ALTER TABLE public.coordination_state DROP CONSTRAINT coordination_state_check;

ALTER TABLE public.coordination_state ADD CONSTRAINT coordination_state_check CHECK (
    (phase =  'none' AND kind IS NULL     AND targets =  '[]'::jsonb AND scope = '[]'::jsonb)
    OR
    (phase <> 'none' AND kind IS NOT NULL AND targets <> '[]'::jsonb)
);
