# Plan 173: Machine credential lifecycle

## What this plan is for

Gives the ops API's machine credentials a lifecycle. Tokens live in a table
rather than an environment variable, are stored as a hash so a database dump
leaks nothing usable, and carry an expiry, a revocation and a last-used
timestamp — so a credential can be retired without guessing what still depends
on it.

## The case

**Raised 2026-09-04**, while wiring Plan 162 Stage 10's CI snapshot download.
That work needed a bearer token in CI for the first time, which turned a
single shared string into a question about three callers, and the question kept
producing better answers than the one it started with.

The *format* half landed immediately, because it had a deadline: named, scoped
entries (`name:scope:token`) went in before any automated caller held a
credential, on the reasoning that a credential format is cheapest to change
while nothing depends on it. Once CI holds a token, changing it means migrating
a live caller against a server being modified at the same time. The storage half
has no such deadline — it sits behind `_resolve_token` and `_tokens_configured`
in [`ops/routers/snapshots.py`](../../ops/routers/snapshots.py), a seam added
deliberately so this plan is one function body rather than a change to the auth
path.

**Revocation is not the hard part; knowing what still depends on a credential
is.** Deleting a row is trivial. Standing in front of `mlflow:read:…` in six
months and deciding whether anything still uses it is not, and an environment
variable can never answer it. `last_used_at` can. That single column is most of
why this is worth building, and it is the one thing the current design cannot
be extended to provide.

**A credential that expires on its own beats one somebody must remember to
kill.** This is the security property that
[OAuth2's](#rejected-alternatives) `client_credentials` grant is usually
reached for, available here without any of its machinery — expiry is a column,
not an authorization server.

**Storing a hash rather than the token is nearly free and changes what a leak
costs.** A `pg_dump` in a backup, a screenshot of pgAdmin, a stray query result
— none of them should hand over a live credential. The presented token is
hashed and the digests compared.

The hash choice is worth stating rather than assuming, because it inverts the
usual advice: **SHA-256 is correct here and bcrypt/argon2 is not.** Password
KDFs are deliberately slow because human passwords are low-entropy and
guessable, so an attacker with the hashes can iterate. A 256-bit random token is
not guessable at any speed, so the slow KDF buys nothing and costs latency on
every authenticated request.

**Human authorization is already table-backed here, which makes machine
credentials the exception rather than the innovation.** `authorized_users`
(V009) has held `email_hash`, `role`, `created_at` and `created_by` since Plan
82; revoking a person is a row change. This plan brings machine callers to where
people already are, and reuses the shape rather than inventing one.

### Why now, when nothing has gone wrong

The trigger is a decision, not an incident. **No token has leaked, and none is
overdue for rotation** — recording otherwise would put a fabricated cause in the
record. What is true is that three callers are arriving (CI, a developer's
laptop, and the Plan 112 MLflow rehearsal), that
[Plan 108](plan_108_deploy_trigger_endpoint.md) drafts a `POST /deploy/trigger`
on this same service whose prerequisites mount the Docker socket into the ops
container — making a `write` scope mean host-level container control — and that
credential lifecycle was judged worth building properly while it is still small.

That is also the honest reason this is a plan rather than a follow-up bolted to
the change that raised it: it is worth doing, it is not urgent, and the
distinction should be visible in the record rather than smuggled into another
plan's stage.

### Rejected alternatives

**OAuth2 `client_credentials`.** Considered and rejected as disproportionate.
Stripped of delegation — which is what OAuth2 exists for, and which
machine-to-machine has none of — the grant amounts to "an API key, plus
expiry." Its scopes buy nothing against one resource server with one
permission, and its revocation story is *worse*, since issued access tokens
outlive the client that was revoked. The cost is an authorization server
(Keycloak, Hydra, Authentik): a stateful service with its own database,
backups and upgrade path, and the property that nothing authenticates when it
is down. That is a larger operational surface than the routes it would protect.

**GitHub Actions OIDC.** Genuinely better than a shared secret, and *not*
rejected — deferred. GitHub mints a short-lived JWT per workflow run, signed by
GitHub, with claims naming the repository and ref; ops validates it against
GitHub's public JWKS and **no shared secret exists at all**. It wants
`PyJWT[crypto]`, JWKS fetching with caching, and issuer/audience/subject
validation, and it covers exactly one of the three callers, so it would run
alongside this rather than replace it. Worth building when eliminating CI's
credential is the goal; this plan is what makes the `ci` entry removable in
isolation when that day comes.

**A bind-mounted tokens file, re-read on mtime change.** Rejected as a half
measure. It removes the container restart from revocation but not the SSH,
which is the larger friction, and it provides no expiry, no `last_used_at` and
no audit trail — so it costs a change to the auth path and buys the least
valuable third of the outcome.

## Design

The credential lookup is already isolated. `_resolve_token` and
`_tokens_configured` in [`ops/routers/snapshots.py`](../../ops/routers/snapshots.py)
are the only two functions that know where a token comes from — the scope
grants, the logging and every test above them read the entry those functions
return. This plan swaps two function bodies. It does not touch the auth path.

### The table is `ops.machine_tokens`, not `ops.snapshot_tokens`

Naming it for its first consumer would be wrong inside one plan's lifetime.
Snapshot downloads are the only caller today, but the mechanism is the ops
service's, and [Plan 108](plan_108_deploy_trigger_endpoint.md)'s deploy trigger
is drafted against the same service. A table named for one route is a table
someone later either renames or works around.

Columns: `name`, `scope`, `token_sha256`, `created_at`, `created_by`,
`expires_at`, `revoked_at`, `last_used_at`.

### Hashing retires the constant-time scan rather than reimplementing it

The current lookup compares the presented token against every configured entry
with no early exit, because breaking on the first match leaks *which caller*
called through response time. Storing a digest removes that problem instead of
carrying it forward: hash what was presented, query the unique index on
`token_sha256`, and there is no per-entry comparison left to time. The one
remaining distinction — a row that exists but is expired versus no row at all —
is information the response already gives away.

`SHA-256`, not a password KDF, for the reason [the case](#the-case) sets out:
these are high-entropy random tokens, so a deliberately slow hash buys no
resistance to guessing and costs latency on every authenticated request.

### `last_used_at` is throttled, or it is a write on every request

Written only when the stored value is already older than a few minutes. The
column exists to answer "is anything still using this credential" before someone
revokes it — a question that tolerates being minutes stale, and does not justify
a write per download.

### The environment fallback survives until the last deploy

A chicken-and-egg, not caution: the first token cannot be issued through an
interface that requires a token. The existing `SNAPSHOT_DOWNLOAD_TOKENS` parsing
stays until every caller holds a table-backed credential, then goes.

**This is why the stage below spans two deploys.** The fallback cannot be
removed by the deploy that introduces the table, because the tokens have to be
issued and the callers repointed in between. A stage that claimed one deploy
would read "done" at the merge, with the environment variable still in
production.

### No administrative interface in this pass

Issuance is a script that prints the plaintext exactly once; revocation is an
`UPDATE`. The script earns its place where raw SQL cannot: a token pasted into
a `psql` session is a token in shell history. A UI does not earn its place yet,
and adding one would be the detail that makes a table with three rows read as
overbuilt.

## Stages

### Stage A — Machine credentials in a table, with a lifecycle

One stage, because the whole change is about a point of work and every
intermediate state — a table with no expiry, an expiry with no way to see what
still depends on the credential — is a state nobody would deploy on purpose.

Create `ops.machine_tokens`; move `_resolve_token` and `_tokens_configured` onto
it; honour `expires_at` and `revoked_at`; record `last_used_at` under the
throttle; add the issuance script. Deploy with the environment fallback still
live, issue a token per caller, repoint CI and local, then deploy again with the
fallback removed.

**Exit:** a table-backed token authenticates in production; every caller has
been reissued one; `SNAPSHOT_DOWNLOAD_TOKENS` and `SNAPSHOT_DOWNLOAD_TOKEN` are
gone from the code and the production `.env`; and a token's plaintext exists in
no database, log or shell history. Demonstrated by revoking a live token and
observing the next request refused **without a restart** — not asserted.

| Order | Stage | Estimate | Status |
|---:|---|---:|---|
| 1 | A — Machine credentials in a table, with a lifecycle | 1 | next |
