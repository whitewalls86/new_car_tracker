-- Plan 142 Stage 1: one durable coordination record for deploys and
-- service/host maintenance. Scope expansion is performed by checked-in code
-- before this immutable snapshot is written.

CREATE TABLE public.coordination_state (
    id integer PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    kind text CHECK (kind IN ('deploy', 'service_maintenance', 'host_maintenance')),
    phase text NOT NULL DEFAULT 'none'
        CHECK (phase IN ('none', 'requested', 'draining', 'active', 'validating')),
    requested_by text,
    reason text,
    targets jsonb NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(targets) = 'array'),
    scope jsonb NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(scope) = 'array'),
    requested_at timestamptz,
    draining_at timestamptz,
    active_at timestamptz,
    validating_at timestamptz,
    completed_at timestamptz,
    expected_work jsonb NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(expected_work) = 'array'),
    manifest_location text,
    operator_notes text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (
        (phase = 'none' AND kind IS NULL AND targets = '[]'::jsonb AND scope = '[]'::jsonb)
        OR
        (phase <> 'none' AND kind IS NOT NULL AND targets <> '[]'::jsonb AND scope <> '[]'::jsonb)
    )
);

INSERT INTO public.coordination_state (id, phase)
VALUES (1, 'none');

GRANT SELECT ON public.coordination_state TO viewer;
GRANT SELECT ON public.coordination_state TO scraper_user;
