INSERT INTO ops.price_observations
                   (listing_id, price, make, model, last_seen_at,
                    last_artifact_id, last_detail_fetched_at)
               VALUES (%s::uuid, 30000, 'honda', 'crv', now(), %s, %s)
