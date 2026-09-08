-- Plan 173 Stage A: machine credentials for the ops API, with a lifecycle.
--
-- Human authorization here has been table-backed since Plan 82: authorized_users
-- (V009) holds a hash, a role and a provenance, and revoking a person is a row
-- change. This brings machine callers -- CI, a developer's laptop, the Plan 112
-- MLflow rehearsal -- to where people already are, and reuses that shape rather
-- than inventing one.
--
-- Named for the service and not for its first route. Snapshot downloads are the
-- only caller today, but the mechanism is the ops service's, and Plan 108's
-- POST /deploy/trigger is drafted against the same service. A table named for
-- one route is a table someone later either renames or works around.

CREATE TABLE ops.machine_tokens (
    id           serial PRIMARY KEY,

    -- For attribution in the access log, and never secret. Deliberately NOT
    -- unique: reissuing a caller's credential leaves the superseded row in
    -- place, revoked, and that history is the audit trail. A unique name would
    -- force the old row to be deleted to issue the new one.
    name         text NOT NULL,

    -- `write` implies `read`; the grant table itself lives in
    -- ops/routers/snapshots.py. The CHECK is here so a scope the service has
    -- never heard of cannot be inserted by hand and then silently fail closed
    -- at every route.
    scope        text NOT NULL CHECK (scope IN ('read', 'write')),

    -- SHA-256 of the token, hex, so a pg_dump in a backup, a screenshot of
    -- pgAdmin or a stray query result hands over nothing usable.
    --
    -- Not bcrypt or argon2, and the inversion of the usual advice is
    -- deliberate: password KDFs are slow because human passwords are
    -- low-entropy and guessable, so an attacker holding the hashes can
    -- iterate. A 256-bit random token is not guessable at any speed, so the
    -- slow hash buys no resistance and costs latency on every authenticated
    -- request.
    --
    -- UNIQUE because it is the lookup key, and that is what retires the
    -- constant-time scan the environment-variable form needed: one indexed
    -- probe has no per-entry comparison whose duration could say which caller
    -- called.
    token_sha256 text NOT NULL UNIQUE,

    created_at   timestamptz NOT NULL DEFAULT now(),
    created_by   text,

    -- A credential that expires on its own beats one somebody must remember to
    -- kill. NULL is permitted so an existing row can be made perpetual by hand,
    -- but nothing issues one: scripts/issue_machine_token.py always sets it.
    expires_at   timestamptz,
    revoked_at   timestamptz,

    -- Most of why this table is worth building. Revocation is not the hard part
    -- -- deleting a row is trivial; standing in front of a credential in six
    -- months and deciding whether anything still uses it is, and an environment
    -- variable can never answer it.
    --
    -- Written under a throttle by the service, so this is minutes-stale by
    -- design. That is the right trade for the question it answers, and it is
    -- why this is not a write on every authenticated request.
    last_used_at timestamptz
);

COMMENT ON TABLE ops.machine_tokens IS
    'Bearer credentials for machine callers of the ops API. Holds a SHA-256 digest, never the token.';

-- ── Permissions ──────────────────────────────────────────────────────────────
--
-- Four roles would otherwise read this table without anyone deciding they
-- should. `ALTER DEFAULT PRIVILEGES IN SCHEMA ops` grants SELECT on new tables
-- to scraper_user (V003), viewer (V004), dbt_user (V007, with DML as well) and
-- airflow_app_user (V015) -- and Flyway creates this table as `cartracker`, the
-- role those defaults were set by, so all four apply here.
--
-- authorized_users avoids this by living in `public`, where no such default
-- exists, and states the intent as an absence of GRANTs. An absence does not
-- work in this schema, so the intent is stated as a REVOKE instead: only the
-- ops service reaches this table. A digest is not a usable credential, but the
-- set of live machine callers and when each last called is not dbt's, the
-- dashboard's, the scraper's or Airflow's to read either.
REVOKE ALL ON ops.machine_tokens FROM scraper_user;
REVOKE ALL ON ops.machine_tokens FROM viewer;
REVOKE ALL ON ops.machine_tokens FROM dbt_user;
REVOKE ALL ON ops.machine_tokens FROM airflow_app_user;
