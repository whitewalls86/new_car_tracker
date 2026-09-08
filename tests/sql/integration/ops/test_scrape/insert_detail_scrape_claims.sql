INSERT INTO detail_scrape_claims (listing_id, claimed_by, status)
               VALUES (%s, %s, %s)
               ON CONFLICT (listing_id) DO UPDATE
                 SET claimed_by = EXCLUDED.claimed_by, status = EXCLUDED.status
