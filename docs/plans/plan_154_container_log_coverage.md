# Plan 154: Container Log Coverage

## What this plan is for

Gets logs from the services that currently produce none into one searchable
place, deciding service by service what is worth collecting before turning any
of it on — so a failure in an unlogged component stops being invisible.

## The case

Split out of the Plan 141 logging health check, and dependent on Plan 141,
which creates the source-policy registry this plan revises.


[`docker-compose.yml`](../../docker-compose.yml) declares 34 services. 28 are in
the default profile, and 26 remain after the one-shots (`flyway`,
`airflow-init`) — but the expected-running set is **28**, because `trawl` and
`redis-trawl` run under the `trawl` profile. They are the live scrape path, so
a profile gate is not absence. The authority is
[`container_health/expected.py`](../../container_health/expected.py), which
`TestExpectedServicesMatchTheManifest` holds equal to Plan 142's
`maintenance-running-set.txt` by exact set equality. Of those 28, **10 have any
path into Loki**:

- Six by application file — `ops`, `scraper`, `processing`, `dbt_runner`,
  `archiver`, `pack-worker`.
- Four by `promtail.enable=true` stdout — `oauth2-proxy`,
  `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`.

**Eighteen have none:** `postgres`, `caddy`, `minio`, `dashboard`, `pgadmin`,
`flaresolverr`, `airflow-triggerer`, `statsd-exporter`, `postgres-exporter`,
`node-exporter`, `prometheus`, `grafana`, `loki`, `promtail`,
`docker-socket-proxy`, `container-health`, `trawl`, `redis-trawl`.

Most of those absences are correct, and Plan 141's source-policy table exists
precisely to say so in writing. The problem is not that 18 services are
excluded. It is that **their exclusion was never a decision** — it is the
residue of which services happened to get a label during the Plan 135 Stage 5
rollout. A correct exclusion and an unconsidered one currently look identical.

Two absences are probably wrong on their merits:

- **`caddy`** is the only edge. Every 4xx and 5xx a user actually receives
  originates there, and none of it is durable. An outage visible to users is
  currently invisible in Loki unless it also broke an instrumented backend.
- **`postgres`** holds every failure mode Plan 123 is still verifying —
  slow queries, connection exhaustion, lock waits. Plan 140's Airflow connection
  budget is the instrument that would show exhaustion; Postgres's own log is
  what would explain it.

**`container-health`** is a weaker third: it is the producer behind Plan 140's
alerts, and a silent producer is a bad failure mode.

The 2026-08-18 evidence in Plan 141 is the counterweight and must stay in view:
a ten-minute unfiltered sample of three Airflow containers produced 486 lines.
Adding services without measuring them is how that happened. This plan
therefore measures before it admits.

## Design

Every one of the 18 uncovered services gets a written classification with a
measured volume figure before any of them is admitted, so an exclusion is a
decision rather than residue. Admission is then incremental and reversible: a
service enters with a drop policy attached and leaves again if seven days of
real volume contradict its projection.

### Principles

1. **Exclusion is a decision with a reason.** Plan 141 makes every service have
   an answer; this plan revisits the answers that were never really chosen.
2. **Measure before admitting.** No service enters Loki without an observed
   lines/day and bytes/day figure and a projected 90-day footprint.
3. **Volume is a filter design problem, not an accept/reject one.** A chatty
   service is a candidate for a drop policy, not automatic exclusion.
4. **The observability stack does not observe itself into a loop.** `loki` and
   `promtail` stay out; local Docker rotation is their diagnostic path.
5. **Coverage is not a target number.** "All 28 services ingested" is a failure
   mode, not a goal.
6. **Privacy travels with the edge.** Caddy access logs carry client IPs and
   full request paths. A retention and redaction policy is a precondition for
   admitting it, not a follow-up.

### Non-goals

- The source-policy registry format, the completeness test, and parsing — all
  Plan 141. This plan supplies revised *entries*, not the mechanism.
- Structured application log fields — Plan 141 Stage 1.
- Dashboards over the resulting streams — Plan 155.
- Loki retention, disk caps, and Docker rotation limits — Plan 135, complete.
- Healthcheck coverage. Plan 140 owns liveness, and its inclusion policy is
  deliberately different; a service can be health-checked and correctly
  unlogged.
- Adding an ingestion path for Airflow DAG/task files, which remain local under
  Plan 135's 30-day policy.

### Test and verification contract

- The Plan 141 completeness test continues to pass with the revised entries; no
  second registry is created.
- Every exclusion entry carries a non-empty reason, enforced by test, following
  the pattern already used by Plan 140's healthcheck deny list in
  [`tests/test_observability_config.py`](../../tests/test_observability_config.py).
- Fixtures for each newly admitted format assert parsed labels or explicit drop.
- A test asserts `loki` and `promtail` are never admitted, with the feedback-loop
  reason recorded inline.
- A test asserts admitted-service labels in Compose and registry entries agree,
  so a label cannot be added without a policy.

## Stages

Sequenced before [`docs/PLAN_DOCUMENT.md`](../PLAN_DOCUMENT.md) landed; adopted
stage letters on 2026-09-07 when its work was chunked into issues. No issue
carries a legacy number yet, so the mapping matters only to inbound prose:

| Legacy | Stage | | Legacy | Stage |
|:---:|:---:|---|:---:|:---:|
| 0 | **A** | | 2 | **C** |
| 1 | **B** | | | |

| Order | Stage | What it delivers | State | Issue |
|---:|:---:|---|---|---|
| 1 | [**A**](#stage-a--decide-every-exclusion-on-its-merits) | Every exclusion decided on its merits, with measured volume | `done` | CAR-92 |
| 2 | [**B**](#stage-b--admit-the-accepted-services) | The accepted services admitted, each with its drop policy | `next` | -- |
| 3 | [**C**](#stage-c--observe-for-seven-days) | Seven days of real volume, and a keep/narrow/remove per service | `--` | -- |

### Stage A — Decide every exclusion on its merits

Read-only, and the substance of this plan.

1. For each of the 18 uncovered services, record: what it logs, at what volume,
   what question its logs would answer, and whether another signal already
   answers it.
2. Classify each as **admit**, **admit with a drop policy**, or **exclude with a
   written reason**. A reason of "not currently labeled" is not a reason.
3. Sample volume directly from `docker logs` over a representative window
   including a scrape cycle and a dbt run. Do not estimate from service type.
4. For `caddy`, additionally determine the access-log format, what PII it
   carries, and whether errors can be separated from routine 2xx traffic at
   ingestion.
5. For `postgres`, determine current `log_min_duration_statement` and related
   settings — the useful content may not be emitted at all today, which would
   make this a Postgres configuration change before it is a Promtail one.

**Exit:** every one of the 18 uncovered services has a written classification --
admit, admit with a drop policy, or exclude with a written reason -- and a
measured lines/day and bytes/day figure taken from `docker logs` over a window
that included a scrape cycle and a dbt run. "Not currently labeled" is not a
reason. Services whose useful content is not emitted today are recorded as
configuration work, not as ingestion work, and `caddy`'s PII answer and
`postgres`'s `log_min_duration_statement` reading are both written down.

Nothing is deployed in this stage, so its exit is a document, not a diff.

### Stage B — Admit the accepted services

1. Add `promtail.enable=true` only to services classified **admit** in Stage A.
2. Give each an ingestion policy in Plan 141's registry — parsing, severity
   mapping, and any drop rule — with its Stage A reason attached.
3. Apply drop policies at ingestion so Promtail's drop counters attribute each
   policy separately, following the pattern Plan 141 establishes.
4. Deploy by recreating Promtail unless a Compose label changed, in which case
   recreate the labeled service too.

**Exit:** every service Stage A classified **admit** is reaching Loki with its
ingestion policy applied, each drop rule attributes to its own Promtail counter,
and no service Stage A excluded has gained a stream. This stage starts Stage C's
seven-day clock, so it is the last point at which the admitted set can change
without restarting that window.

### Stage C — Observe for seven days

1. Measured lines/day and bytes/day per newly admitted service against the
   Stage A projection.
2. Recomputed 90-day Loki footprint and disk headroom against Plan 135's bounds.
3. Confirmation that each new stream's drop policy is doing the expected amount
   of work and no more.
4. One representative error per admitted service proven to reach the error
   view.

#### Stage C decision

Per admitted service: **keep**, **narrow the filter**, or **remove**. A service
whose seven-day volume exceeds its Stage A projection by a stated margin is
narrowed or removed rather than absorbed.

**Exit:** seven days observed, with a recorded keep/narrow/remove decision per
admitted service, a recomputed 90-day Loki footprint inside Plan 135's bounds,
and one representative error per admitted service proven to reach the error
view. A projection missed by more than its stated margin is acted on here, not
carried.

## Relationship to other plans

- **Plan 141 is a hard prerequisite.** It builds the registry, the completeness
  test, and the parsing contract. This plan is the first substantive revision of
  its contents and should not start before it lands.
- **Plan 135 set the bounds** this plan spends against. Its retention and disk
  caps are the budget; Stage C verifies the spend fits.
- **Plan 140 owns healthchecks.** Its service set and this one differ on
  purpose; neither test may be rewritten to match the other.
- **Plan 141 Stage 1 supplies application log structure.** That is a separate
  concern from this plan, which is about which containers are heard at all, not
  what they say.
- **Plan 155 presents the result.** More streams make its per-service breakdown
  more necessary, not less.

## Success criteria

1. All 28 expected-running services have a written, reasoned classification;
   none is excluded merely by omission.
2. Every admitted service has a measured volume figure predating its admission.
3. `caddy` and `postgres` are resolved explicitly — admitted with a policy, or
   excluded with a reason that survives review.
4. Seven days of evidence support a keep/narrow/remove decision per admitted
   service.
5. The 90-day Loki footprint after admission stays within Plan 135's bounds with
   recorded headroom.
6. No second coverage registry exists.

## Rollback and safe stopping points

- **After Stage A:** classifications recorded, no runtime change. This is a
  legitimate finish if every answer is "exclude".
- **After Stage B:** remove the labels and registry entries; Promtail returns to
  its Plan 141 state.
- **After Stage C:** the per-service decision is the durable output.

## Record

### Stage A — Decide every exclusion on its merits (2026-09-07)

All eighteen uncovered services classified against a 24-hour `docker logs`
window ending 2026-09-07T19:27Z, sampled on the production host. The window
covered 139,747 `scraper` lines and 8,727 `dbt_runner` lines, so it was
representative rather than quiet. Volume was measured per container rather than
estimated from service type, as item 3 required.

**Two admits, sixteen exclusions.**

| Service | lines/day | bytes/day | Verdict | Reason |
|---|---:|---:|---|---|
| `trawl` | 448 | 23 KB | **admit** | Turnstile solver challenge events, including 31 `cf_clearance set but still on challenge page`. The live scrape path, and the only place the 2026-08-14 silent solver failure would have been legible. 23 KB/day is negligible |
| `postgres` | 595 | 109 KB | **admit, with a drop policy** | 288 daily checkpoint records are already covered by postgres-exporter and drop. The residue is where a 16-hour broken hourly job and a real connection exhaustion were recorded and never seen |
| `docker-socket-proxy` | 187,218 | 45.4 MB | exclude | HAProxy access log of every Docker API metadata read, all `200`. No durable event contract. 89% of all lines and 94% of all bytes across the eighteen; the volume itself is Plan 177 Stage A |
| `container-health` | 8,633 | 512 KB | exclude | 100% uvicorn access records — `/metrics` ×5,760, `/health` ×2,871 — and nothing about the service's own work. Emission is Plan 177 Stage B |
| `grafana` | 3,509 | 1.0 MB | exclude | Alert-router chatter. Exactly two genuine `level=error` lines in 24 hours, both the same benign missing-plugin notice; the 866 `error` string matches are `error=null` fields |
| `caddy` | 2,885 | 712 KB | exclude | No access logging exists to admit. 2,876 of 2,885 lines are its own healthcheck echo. Admission is Plan 177 Stages D and E |
| `pgadmin` | 2,877 | 253 KB | exclude | 100% `GET /pgadmin/misc/ping` from its own Wget healthcheck |
| `airflow-triggerer` | 2,876 | 400 KB | exclude | 100% `N triggers/watchers currently running` at info level, every 30 seconds |
| `promtail` | 126 | 18 KB | exclude | File-watcher events only, and principle 4 — the collector does not ingest itself |
| `redis-trawl` | 120 | 8.6 KB | exclude | RDB background saves only |
| `prometheus` | 81 | 13 KB | exclude | TSDB head GC and WAL checkpoints. Self-health is metric based |
| `minio` | 0 | 0 | exclude | Startup banner only — 159 lines all-time, last on 2026-08-31 |
| `node-exporter` | 0 | 0 | exclude | Startup banner only — 114 lines all-time |
| `flaresolverr` | 0 | 0 | exclude | Startup banner only — 27 lines all-time. Also not the active scrape path |
| `postgres-exporter` | 0 | 0 | exclude | Startup banner only — 15 lines all-time |
| `statsd-exporter` | 0 | 0 | exclude | Startup banner only — 9 lines all-time |
| `dashboard` | 0 | 0 | exclude | Startup banner only — 8 lines all-time |
| `loki` | 0 | 0 | exclude | Principle 4 — ingesting Loki into Loki is a feedback loop. 1,393 lines all-time with occasional genuine errors, diagnosed through local Docker rotation |

**Seven services log only at boot.** `minio`, `node-exporter`, `flaresolverr`,
`postgres-exporter`, `statsd-exporter` and `dashboard` last emitted a line on
2026-08-31 and have been silent for the seven days since. Nothing to ingest is
the cleanest exclusion available, and none of them had been considered before.

**`caddy`'s PII answer: there is none today, because there is no access
logging.** No `log` directive exists anywhere in the `Caddyfile`, verified by
grep rather than inferred from reading. Of 2,885 daily lines, 2,876 are the
Compose healthcheck's `GET /config/` against the admin API on `127.0.0.1` and
the remaining 8 are ACME renewal and TLS storage maintenance. The only IP the
log contains is `127.0.0.1` and the only path is `/config/`. The privacy
question is therefore not answerable as a measurement — it becomes real the
moment logging is turned on, which is Plan 177 Stage D's subject.

**`postgres`'s reading.** `log_min_duration_statement = -1`,
`log_lock_waits = off`, `log_connections` and `log_disconnections` both `off`,
`log_statement = none`. Every failure mode this plan named as the reason to
want Postgres logs is switched off. What is emitted is `log_checkpoints = on`,
`log_autovacuum_min_duration = 600000`, and errors after the fact. One piece of
good news: `log_destination = stderr` with `logging_collector = off`, so the
output already reaches container stdout and admission needs a Promtail label
and no file plumbing.

**A drop policy for `postgres` is harder than it looks.** Most of its error
lines are ad-hoc `psql` sessions guessing at table names, role names and
quoting — `column " receipts | " does not exist` is pasted table output, and
`syntax error at or near "\"` recurs across twenty days. Three classes are
genuinely separable and worth keeping: `too many connections`, integrity and
check-constraint violations, and the same relation failing on a schedule. The
last of those is the awkward one, because `relation ... does not exist` appears
in both columns and what separates them is periodicity, which Promtail cannot
evaluate at ingestion. Stage B either ingests that class and lets a Grafana
rule find the periodicity, or drops it and accepts missing the case below.

**What the stage found that it was not looking for.** Postgres's log holds one
genuine incident in twenty days. On 2026-08-29 `metrics_user` — postgres-
exporter's own account, capped at `CONNECTION LIMIT 3` by Plan 86's V033 —
exhausted its limit, and `up{job="postgres"}` read 0 from 05:57 to 06:13. The
source-policy registry excludes Postgres on the grounds that "database health
and capacity are covered by postgres-exporter metrics"; the one event in twenty
days that tests that claim is postgres-exporter going blind, and the only
durable record of why is the log the exclusion keeps out. Filed as Plan 176.

A second finding is already closed: `staging.coordination_state_events` and
`staging.coordination_release_evidence` failed exactly twice an hour, on the
hour, for sixteen consecutive hours on 2026-08-26/27. Both relations exist now.
Sixteen hourly failures at a fixed cadence is the clearest case in the dataset
of something a retained Postgres log would have caught on hour two.

**Downstream.** Four services turned out to be emission-configuration work
rather than ingestion work and were split into
[Plan 177](plan_177_service_log_emission.md), Stages A–E (CAR-97 to CAR-101).
The connection-limit defect became [Plan 176](plan_176_role_connection_limits.md),
backlog. Neither is Stage B's problem, which is the point of separating them.

**Stage B is smaller than this plan assumed.** Two admits totalling under 1 MB
a day before filtering leaves the seven-day footprint well inside Plan 135's
bounds — but it also means Stage C's observation window will be measuring very
little.

Public surfaces: no mechanism, name or quantity either surface states was
changed by this work.

Cost: estimate 3 → actual 1 (−2). Read-only throughout; nothing was deployed,
so this stage's output is a document rather than a diff.

Full sampling method, per-service line-shape composition, the Postgres error
forensics and the Prometheus queries:
[`plan_154_stage_A_evidence.md`](../evidence/plan_154_stage_A_evidence.md).
