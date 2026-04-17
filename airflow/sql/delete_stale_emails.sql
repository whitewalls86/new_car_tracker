-- Kept in a .sql file rather than inline so the integration test can load the
-- exact SQL without importing the DAG module (which would require Airflow installed).
UPDATE access_requests
SET notification_email = NULL
WHERE notification_email IS NOT NULL
  AND requested_at < now() - interval '48 hours'
