-- Take the legacy deploy_intent lock, or take it over once it has gone stale.
-- RETURNING is the whole result: a row means this caller won, no row means the
-- lock is held and still fresh.
--
-- Note the third parameter sits inside the quoted interval literal
-- (`interval '%s minutes'`). psycopg2 substitutes into the statement text
-- before the server parses it, so STALE_LOCK_MINUTES lands as the number of
-- minutes. Preserved as-is: this is the statement production runs.
UPDATE deploy_intent
   SET
        intent = 'pending',
        requested_at = now(),
        requested_by = %s,
        pause_long_jobs = %s
   WHERE id = 1
     AND (intent = 'none'
          OR requested_at < now() - interval '%s minutes')
   RETURNING intent;
