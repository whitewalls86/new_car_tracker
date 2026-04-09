INSERT INTO public.deploy_intent (id, intent, requested_at, requested_by)
VALUES (1, 'none', NULL, NULL)
ON CONFLICT (id) DO NOTHING;
