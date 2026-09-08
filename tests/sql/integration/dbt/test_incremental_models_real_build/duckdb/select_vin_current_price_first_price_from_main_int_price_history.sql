select vin, current_price, first_price, min_price, max_price, total_price_observations, price_drop_count, price_increase_count, first_seen_at, last_seen_at from main.int_price_history order by vin
