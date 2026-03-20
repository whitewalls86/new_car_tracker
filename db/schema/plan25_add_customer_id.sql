-- Plan 25.2: Add customer_id to detail_observations
-- Run this BEFORE deploying the n8n workflow change.

ALTER TABLE detail_observations ADD COLUMN IF NOT EXISTS customer_id text;

CREATE INDEX IF NOT EXISTS ix_detail_observations_customer_id
  ON detail_observations (customer_id) WHERE customer_id IS NOT NULL;
