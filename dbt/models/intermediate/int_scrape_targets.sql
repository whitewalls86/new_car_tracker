-- Active scrape targets: the canonical make/model pairs we track.
-- Joins the seed lookup (slug → display name) to enabled search_configs.

-- int_scrape_targets
SELECT DISTINCT ON (sc.search_key)
  sc.search_key,
  sc.enabled,
  sc.make_slug,
  sc.model_slug,
  COALESCE(obs.make, sc.make_slug) as make,
  COALESCE(obs.model, sc.model_slug) as model
FROM {{ ref('stg_search_configs') }} sc
LEFT JOIN {{ ref('stg_raw_artifacts') }} ra
  ON ra.search_key = sc.search_key
LEFT JOIN {{ ref('stg_srp_observations') }} obs
  ON obs.artifact_id = ra.artifact_id
WHERE
    obs.make IS NOT NULL
    AND sc.enabled = true
ORDER BY sc.search_key, obs.fetched_at DESC

