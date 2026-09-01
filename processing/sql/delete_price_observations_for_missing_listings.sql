-- Drop price observations for VINs in this batch whose listing_id is no longer
-- among those observed, so a relisted VIN does not keep its stale rows.
DELETE FROM ops.price_observations
 WHERE vin = ANY(%s)
   AND listing_id NOT IN (
       SELECT unnest(%s::uuid[])
   )
