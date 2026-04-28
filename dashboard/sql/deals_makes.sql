SELECT DISTINCT make
FROM mart_deal_scores
WHERE (make, model) IN (SELECT make, model FROM int_active_make_models)
ORDER BY make
