# Plan 177: Service Log Emission

## What this plan is for

Fixes what four services write to their logs, so the record answers the
questions asked of it. The edge records no user-facing errors, the database no
slow queries or lock waits, the health producer only its own web traffic —
while a fourth floods 45 MB a day of routine proxy noise.

## The case

Split out of [Plan 154](plan_154_container_log_coverage.md) Stage A on
2026-09-07, which measured all eighteen services with no path into Loki and
classified each one. Four of them could not be classified on ingestion grounds
at all, because the problem is not whether anyone is listening — it is that
what they say does not answer the question.

Plan 154 draws that boundary itself: it is about "which containers are heard at
all, not what they say." Changing what a service says is configuration work on
that service, and it belongs here.

### What was measured

A 24-hour window ending 2026-09-07T19:27Z, sampled from `docker logs` on the
production host. The window covered heavy scraper and dbt activity, so it is
representative rather than quiet.

| Service | lines/day | bytes/day | What is actually in it |
|---|---:|---:|---|
| `docker-socket-proxy` | 187,218 | 45.4 MB | HAProxy access log of every Docker API call, all `200` |
| `container-health` | 8,633 | 512 KB | uvicorn access log only — `/metrics` ×5,760, `/health` ×2,871 |
| `caddy` | 2,885 | 712 KB | 99.7% its own healthcheck echo, plus 8 ACME lines |
| `postgres` | 595 | 109 KB | 288 checkpoint records, and errors only when something already failed |

### The three that say too little

**`caddy` has no access logging at all.** There is no `log` directive anywhere
in the `Caddyfile`, so Caddy emits only its own runtime log. Of 2,885 lines in
the window, 2,876 are the Compose healthcheck's `GET /config/` against the
admin API on `127.0.0.1`, once every 30 seconds; the remaining 8 are ACME
renewal and TLS storage maintenance. Caddy is the only edge, so **every 4xx and
5xx a user actually receives originates there and none of it is recorded**. An
outage visible to users is invisible unless it also broke an instrumented
backend.

This is also where the privacy question lives. Access logs carry client IPs and
full request paths, and this is the first thing in the system that would store
either durably. Turning logging on and deciding the redaction policy are the
same decision, not two.

**`postgres` has every useful setting switched off.** Live `pg_settings` reads
`log_min_duration_statement = -1`, `log_lock_waits = off`, `log_connections`
and `log_disconnections` both `off`, and `log_statement = none`. Slow queries,
lock waits and connection exhaustion — the failure modes Plan 154 names — are
never emitted. What it does emit is `log_checkpoints` and errors after the
fact.

One consequence is specific enough to name. Plan 154 Stage A found that most of
Postgres's error lines are ad-hoc `psql` sessions guessing at table and role
names, mixed in with the few that are real. `log_connections` and
`log_disconnections` are what would let an ingestion rule tell an interactive
session from a service connection — which is the discriminator a Postgres drop
policy needs and does not have.

**`container-health` records only its own web traffic.** All 8,633 lines are
uvicorn access records: 5,760 `GET /metrics` from Prometheus and 2,871
`GET /health` from the Compose healthcheck. There is nothing about what the
service actually does. It is the producer behind Plan 140's alerting contract,
and a silent producer is a bad failure mode — Plan 154's own text flags it as a
candidate for exactly this reason.

### The one that says far too much

**`docker-socket-proxy` emits 187,218 lines and 45.4 MB per day** — HAProxy
access logging of every Docker API call, all returning `200`. That is **89% of
all lines and 94% of all bytes** produced by the eighteen services Plan 154
examined, and none of it carries information. At the host's `50m × 3` rotation
it turns over roughly every 26 hours and holds ~150 MB resident.

It is the same defect inverted: the content does not match the question. It
also has a disk dimension that connects to Plan 135, which found 12.3 GiB of
unrotated container logs filling the root filesystem.

### Why one plan

The four share a single question — *does the content this service emits answer
the question we would ask it?* — and a single shape of answer, which is a
configuration change to the service rather than to Promtail. Splitting them
means writing that analysis four times for four small changes.

They are not equally risky, and that is a sequencing matter rather than a
reason to separate them: `caddy` is the only edge and Plan 138 calls its site
block the riskiest edit in the repository, and `postgres` settings live in the
Compose `command:` block, where applying them recreates the container and
restarts the database.

## Design

Fix emission at the source, one service at a time, ordered cheapest and least
risky first, with the edge last and behind an explicit policy decision. Each
stage is a single service, deploys on its own, and reverts on its own — so
every stage boundary is a safe stopping point and no rollback drags three
unrelated services with it.

The ordering is deliberate rather than incidental. `docker-socket-proxy` is a
logging-verbosity change with no privacy dimension and no user-facing surface;
`caddy` is the only edge, carries the client-IP decision, and Plan 138 calls
its site block the riskiest edit in the repository. Working outward from the
cheapest lands three improvements before the hard decision has to be made, and
leaves the risky change standing alone where its own deploy can be watched.

Alternatives rejected on the way here:

- **Fold this into [Plan 154](plan_154_container_log_coverage.md) Stage B.**
  That plan's boundary is which containers are heard at all, not what they say.
  Folding these in would also put a database restart and the riskiest edit in
  the repository inside a plan whose Stage A was read-only and deploy-free.
- **One observability-configuration sweep in a single deploy.** Four services
  with very different blast radii. A single deploy makes the edge change ride
  along with the cheap one, and a rollback reverts all four.
- **Fix it in the collector instead.** Promtail cannot invent content that was
  never emitted. Plan 141 owns the collector contract; every change here is
  upstream of it.
- **Do `caddy` first because it is worth the most.** It is also the riskiest,
  and the only one blocked on a privacy policy nobody has decided yet.

## Stages

| Order | Stage | What it delivers | State | Issue |
|---:|:---:|---|---|---|
| 1 | [**A**](#stage-a--turn-down-docker-socket-proxy-access-logging) | 45 MB/day of routine proxy logging stopped at source | `next` | CAR-97 |
| 2 | [**B**](#stage-b--make-container-health-log-its-work) | The Plan 140 producer says what it actually does | `--` | CAR-98 |
| 3 | [**C**](#stage-c--enable-the-postgres-diagnostic-settings) | Slow queries, lock waits and connections emitted | `--` | CAR-99 |
| 4 | [**D**](#stage-d--decide-the-caddy-access-log-and-redaction-policy) | The edge logging and redaction policy, decided | `--` | CAR-100 |
| 5 | [**E**](#stage-e--implement-caddy-access-logging) | User-facing 4xx and 5xx recorded for the first time | `--` | CAR-101 |

### Stage A — Turn down docker-socket-proxy access logging

1. Reduce HAProxy access logging so routine `200` metadata reads stop being
   recorded line by line. Decide explicitly between turning logging down and
   turning it off: off is cheaper and loses the ability to see unexpected
   Docker API access, which is the one thing this container's log could be
   worth.
2. Recreate the service, since this is a Compose-level change.
3. Re-measure over a window comparable to the Stage A baseline.

**Exit:** measured lines/day and bytes/day for `docker-socket-proxy` fall at
least an order of magnitude from the 2026-09-07 baseline of 187,218 lines and
45.4 MB per day, and Plan 140's health metric still reads healthy for it.

### Stage B — Make container-health log its work

1. Emit an application-level event for the work the service performs, rather
   than only the uvicorn access record of being asked for it.
2. Silence or demote the `/metrics` and `/health` access lines, which are
   5,760 and 2,871 records a day of a Prometheus scrape and a healthcheck
   asking the same question on a timer.

**Exit:** a representative health-evaluation event appears in the container's
log, `/metrics` and `/health` access records no longer dominate it, and a new
lines/day figure is recorded against the 8,633 baseline.

### Stage C — Enable the Postgres diagnostic settings

1. Choose values for `log_min_duration_statement`, `log_lock_waits`,
   `log_connections` and `log_disconnections`, and say what question each one
   is being turned on to answer.
2. Choose the mechanism. The settings live in the Compose `command:` block,
   where applying them recreates the container and restarts the database;
   `ALTER SYSTEM` plus `pg_reload_conf()` avoids the restart but puts the value
   somewhere the repository does not record. Whichever is chosen, record why.
3. `log_connections` and `log_disconnections` carry a second purpose worth
   stating: they are what would let an ingestion rule tell an ad-hoc `psql`
   session from a service connection, which is the discriminator a Postgres
   drop policy needs and does not have.

**Exit:** the four settings read their intended values in `pg_settings` on the
production database, a deliberately slow query appears in the log, and the
mechanism used is recorded together with the restart cost it did or did not
incur.

### Stage D — Decide the Caddy access-log and redaction policy

Decision only. Nothing is deployed in this stage, so its exit is a document,
not a diff.

1. Determine the log format and where the directive belongs — global, per-site,
   or per-handle, given that the public and authenticated routes have different
   exposure.
2. Decide which statuses are emitted at all. Emitting everything and dropping
   `2xx` at ingestion attributes the volume to a Promtail drop counter, which is
   the pattern Plan 141 established; never emitting `2xx` is the stronger
   privacy position but removes the ability to tell an outage from an absence of
   visitors.
3. Decide the client-IP treatment — kept, hashed, or dropped — and where
   redaction happens. This is the first thing in the system that would durably
   store client IPs and full request paths for real users.

**Exit:** a written decision covering format, emitted statuses, client-IP
treatment and the point at which redaction is applied, with the reasoning for
each recorded rather than only the outcome.

### Stage E — Implement Caddy access logging

1. Add the `log` directive as Stage D specified. There is none in the
   `Caddyfile` today.
2. Apply the redaction Stage D chose.
3. Deploy with `docker compose up -d caddy`, which recreates the container —
   `restart` reuses the old configuration.

**Exit:** a deliberate 4xx and a deliberate 5xx each appear in `caddy`'s log in
the shape Stage D specified and with its redaction applied, every public route
still serves, and a measured lines/day and bytes/day figure is recorded against
the 2,885 baseline.
