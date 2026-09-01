-- Flip a search config's enabled flag without reading it first.
-- NOT enabled rather than a supplied value, so two concurrent toggles cannot
-- both write the same state from a stale read.
UPDATE search_configs
   SET enabled = NOT enabled, updated_at = now()
 WHERE search_key = %s
