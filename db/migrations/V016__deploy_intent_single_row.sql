-- Enforce that deploy_intent can never have more than one row.
-- dbt_lock already has this constraint (added in V001); deploy_intent was missed.
ALTER TABLE deploy_intent ADD CONSTRAINT deploy_intent_single_row CHECK (id = 1);
