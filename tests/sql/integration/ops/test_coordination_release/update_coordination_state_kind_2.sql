UPDATE coordination_state
              SET kind='host_maintenance', phase='validating', generation=generation + 1,
                  targets='["host"]'::jsonb, scope='["host"]'::jsonb,
                  requested_by='integration', manifest_location='/integration/manifest'
            WHERE id=1
        RETURNING generation
