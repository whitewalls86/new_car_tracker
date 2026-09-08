UPDATE deploy_intent
               SET intent = 'pending', requested_at = now(), requested_by = %s
               WHERE id = 1
                 AND (intent = 'none'
                      OR requested_at < now() - interval '%s minutes')
               RETURNING intent
