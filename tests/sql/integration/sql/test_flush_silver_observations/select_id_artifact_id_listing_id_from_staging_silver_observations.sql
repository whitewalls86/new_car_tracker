SELECT id,
                      artifact_id, listing_id, vin, canonical_detail_url,
                      source, listing_state, fetched_at,
                      price, make, model, trim, year, mileage, msrp,
                      stock_type, fuel_type, body_style,
                      dealer_name, dealer_zip, customer_id, seller_id,
                      dealer_street, dealer_city, dealer_state, dealer_phone,
                      dealer_website, dealer_cars_com_url, dealer_rating,
                      financing_type, seller_zip, seller_customer_id,
                      page_number, position_on_page, trid, isa_context,
                      body, condition
               FROM staging.silver_observations
               WHERE id <= (SELECT MAX(id) FROM staging.silver_observations)
