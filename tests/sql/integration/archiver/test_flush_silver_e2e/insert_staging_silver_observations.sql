INSERT INTO staging.silver_observations
                   (artifact_id, listing_id, source, listing_state, fetched_at, vin,
                    price, make, model)
               VALUES (999999, %s, 'srp', 'active', '2026-01-10 12:00:00+00',
                       'E2EFLUSHTEST00001', 25000, 'Test-Make', 'Test-Model')
               RETURNING id
