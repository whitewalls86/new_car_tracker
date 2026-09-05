UPDATE coordination_state SET kind='host_maintenance', phase='active', targets='["host"]'::jsonb, scope='["database"]'::jsonb WHERE id=1
