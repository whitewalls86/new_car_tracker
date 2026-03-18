-- Example search configuration: Honda CR-V Hybrid
-- Adjust zip, radius, and models to your preferences.
-- See /admin UI (http://localhost:8000/admin) for interactive config management.

INSERT INTO search_configs (search_key, enabled, source, params)
VALUES (
  'honda-cr_v_hybrid',
  true,
  'cars.com',
  '{
    "zip": "77080",
    "makes": ["honda"],
    "models": ["honda-cr_v_hybrid"],
    "scopes": ["local", "national"],
    "sort_order": "listed_at_desc",
    "max_listings": 2000,
    "radius_miles": 200,
    "sort_rotation": ["list_price", "listed_at_desc", "best_deal", "best_match_desc"],
    "max_safety_pages": 500
  }'::jsonb
)
ON CONFLICT (search_key) DO NOTHING;
