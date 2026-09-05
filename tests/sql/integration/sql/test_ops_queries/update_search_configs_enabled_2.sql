UPDATE search_configs SET enabled = NOT enabled, updated_at = now() WHERE search_key = %s
