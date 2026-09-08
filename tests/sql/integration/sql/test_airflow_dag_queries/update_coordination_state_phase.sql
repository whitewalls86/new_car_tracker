UPDATE coordination_state    SET phase = %s, scope = %s::jsonb, generation = %s,        kind = %s, targets = %s::jsonb  WHERE id = 1
