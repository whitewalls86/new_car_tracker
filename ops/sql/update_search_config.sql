-- Full update of a search config from the admin UI.
-- Every mutable field is written, so a partial edit in the UI must send the
-- unchanged values back rather than omitting them.
UPDATE search_configs
   SET enabled = %s,
       params = %s::jsonb,
       rotation_order = %s,
       rotation_slot = %s,
       updated_at = now()
 WHERE search_key = %s
