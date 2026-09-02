-- Soft delete: disable the config and rename its key so the original is free
-- to be reused. The caller supplies the retired key as the first parameter and
-- the current key as the second.
UPDATE search_configs
   SET enabled = false, search_key = %s, updated_at = now()
 WHERE search_key = %s
