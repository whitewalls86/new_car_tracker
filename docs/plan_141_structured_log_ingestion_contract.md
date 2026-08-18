# Plan 141: Structured Log Ingestion and Dashboard Contract

## Status

DRAFT, written 2026-08-18 while closing Plan 135 Stage 5. Production is stable
and the storage bounds are live; this plan is a correctness and usability
follow-up, not an incident response and not a reason to reopen Plan 135.

Priority **85 (high)**. Effort **S plus a 24-hour production soak**.

## Why this is a separate plan

Plan 135 answered the storage question: every local log sink is bounded, Loki
retains 90 days, and Airflow task logs retain 30 days. Its Stage 5 rollout also
made selected Docker stdout durable. That exposed a different question:

> Once a line reaches Loki, can a person and every dashboard reliably tell what
> produced it, how severe it is, and whether it was intentionally retained?

The answer is currently no. Solving that is log-schema and dashboard-contract
work, not storage-retention work.

## Production evidence from 2026-08-18

The first Docker discovery configuration admitted all stdout from the three
Airflow control-plane containers. A ten-minute sample contained 486 lines,
dominated by routine DAG serialization, polling, and scheduling messages. The
configuration was narrowed before final deployment and successful OAuth
`/oauth2/auth` subrequests were also dropped.

The post-deploy audit then found the deeper contract gaps:

- Airflow warnings are retained as message text but have no `level` label, so
  they appear in **All Service Logs** and not **Error / Warning Logs**.
- The Airflow filter searches arbitrary text. It retains an INFO record from
  logger `uvicorn.error` and a statistics header containing `# Errors`, neither
  of which is an error event.
- The Error / Warning panel matches `ERROR|WARNING` and omits `CRITICAL`.
- OAuth access records have no parsed status or severity. A failed auth request
  therefore has no defined route into the error panel.
- The six file jobs glob `app.log*`. On the first persistent-positions rollout,
  Promtail read rotated legacy files as well as the active file. Legacy
  plaintext failed the JSON and timestamp stages, so old May records arrived as
  current, unlabeled events. In one 15-minute query this produced **6,216
  unlabeled Archiver lines and 715 unlabeled Ops lines**.
- The current tests prove YAML shape and exact service sets. They do not prove
  the behavior of representative log records or compatibility with dashboard
  selectors.

Promtail's positions are now persistent, the one-time replay settled, and the
immediate ingestion flood is stopped. These findings therefore justify planned
work, not another emergency production edit.

## The contract

Every intentionally retained event must have:

1. `service` — stable producer identity.
2. `source` — at least `application_file` or `container_stdout`.
3. `level` — normalized `DEBUG|INFO|WARNING|ERROR|CRITICAL` whenever the source
   has severity semantics.
4. A documented inclusion, exclusion, and privacy policy for its source.
5. A parser whose output is exercised by the same selectors the dashboards use.

Unparsed data must not silently look classified. It is either dropped by an
explicit source policy or labeled/countable as unclassified.

### Source policy

| Source | Intended policy |
|---|---|
| JSON application files (`ops`, `scraper`, `processing`, `dbt_runner`, `archiver`, `pack-worker`) | Tail the active `app.log` only; retain all levels; parse timestamp, level, and logger |
| Airflow apiserver, scheduler, and DAG processor stdout | Retain parsed `WARNING`, `ERROR`, and `CRITICAL` only; routine and unparsed control-plane chatter is not durable telemetry |
| OAuth2 Proxy stdout | Drop successful `/oauth2/auth` subrequests; retain authentication failures and interactive/session events; parse HTTP status where present |
| Loki stdout | Do not ingest into Loki; keep Loki itself at `warn` and rely on bounded Docker rotation for local diagnostics |
| Other container stdout | Excluded unless a written use case and expected volume are added to this policy |
| Airflow DAG/task files | Remain local under the Plan 135 30-day cleanup policy; they are not a second Loki ingestion path |

This table, not “every service gets a Promtail job,” is the coverage invariant.
Some services intentionally log through files, some through selected stdout,
and some should not enter Loki at all.

## Stages

### Stage 0 — Freeze representative fixtures and a baseline

Before changing pipelines:

1. Add sanitized fixtures for current JSON application logs, each Airflow
   severity, Airflow multiline/control-plane noise, OAuth 2xx/4xx/5xx access
   records, OAuth lifecycle messages, and malformed input.
2. Record 24-hour counts and bytes by `service`, `source`, and `level`, including
   an explicit missing-label query.
3. Record Loki volume size and calculate the projected 90-day footprint from
   the measured post-replay rate. A time retention setting is not a capacity
   guarantee.

Stage 0 is read-only in production.

### Stage 1 — Make ingestion semantic

1. Change application file targets from `app.log*` to active `app.log`. The
   application already uses `RotatingFileHandler(maxBytes=5_000_000,
   backupCount=3)`; Promtail should follow the active file through rotation, not
   discover every backup as another source.
2. Add `source=application_file` to file jobs and
   `source=container_stdout` to Docker discovery.
3. Parse Airflow's bracketed severity, normalize it to uppercase, attach the
   `level` label, and filter using that parsed label. Do not infer severity from
   words in logger names or message bodies.
4. Define the treatment of traceback continuation lines in fixtures before
   implementation. Either join them to the initiating record or document their
   exclusion; do not retain them accidentally.
5. Parse OAuth HTTP status. Map 4xx authentication failures to `WARNING` and
   5xx failures to `ERROR`; keep the existing narrow drop for successful
   `/oauth2/auth` subrequests. Give other intentionally retained lifecycle
   records an explicit classification policy.
6. Keep drop reasons distinct so Promtail counters show which policy is doing
   work.

### Stage 2 — Make dashboards consume the contract

1. Change Error / Warning Logs to include `CRITICAL`.
2. Ensure retained Airflow warnings/errors and OAuth failures appear there from
   their labels, not from a second text-search implementation.
3. Split volume by `service`, `source`, and `level` so an empty level cannot hide
   in an unlabeled legend.
4. Add a small contract/coverage panel or query for retained records missing
   `source` or an expected `level`. Its healthy value is zero.
5. Preserve All Service Logs as the broad diagnostic view, with service/source
   filters if the combined stream becomes hard to use.

### Stage 3 — Test behavior, not only configuration shape

1. Run every Stage 0 fixture through the intended parser policy and assert the
   output labels or explicit drop result.
2. Assert dashboard selectors match retained warning/error/critical fixtures
   and do not match INFO fixtures.
3. Assert the source-policy table in code covers every Compose service with one
   of: application file, selected stdout, intentional exclusion, or transient
   exemption. Every exclusion carries a reason.
4. Validate the real Promtail image configuration in CI or a deterministic
   validation script; YAML parsing alone is insufficient.
5. Keep Plan 140's healthcheck coverage test separate. Logging and liveness
   have different inclusion policies.

### Stage 4 — Deploy, soak, and accept

Deploy by recreating Promtail only unless a Compose label changes. Afterward:

1. Promtail has no discovery, parsing, out-of-order, too-old, or empty-label
   errors after startup settles.
2. Persistent positions advance across a Promtail recreation without replay.
3. Missing `source` and expected-`level` queries remain zero for 24 hours.
4. Inject or safely produce one representative warning/error per selected
   source and prove it appears in Error / Warning Logs.
5. Routine Airflow and successful OAuth auth subrequests remain absent.
6. Compare measured bytes/day with Stage 0 and record the projected 90-day Loki
   footprint and disk headroom.

Do not rewrite or selectively delete the misclassified historical Loki data.
Let the 90-day retention policy age it out unless capacity evidence creates a
separate, explicitly approved deletion need.

## Intersections and sequencing

### Plan 135 — storage observability

Plan 135 owns byte/inode visibility, Docker and journald caps, Loki retention,
Airflow task-log pruning, and the maintenance runbook. It is complete without
this plan. Plan 141 consumes those bounds and verifies that the contents placed
inside them are useful.

### Plan 136 — solver recycle and real liveness

Plan 136 remains ahead of Plan 141. Its critical signals are Prometheus
liveness, freshness, and solver outcome counters; they must not be delayed for
dashboard cleanup. The intersection is narrow:

- Revalidate `ct-403-log-spike` after Stage 1 because it is the Plan 136 alert
  that depends on Loki text.
- Plan 136 warning-only observations should name the expected `service` and
  `level` contract rather than use unconstrained text queries.
- The short Airflow HMAC-key warning discovered during this audit is a real
  configuration finding, not log noise. Route its remediation to Plan 136
  Stage 0's Airflow configuration slice (or a security plan); do not suppress
  it here.
- Plan 136's container-health producer and restart authority do not belong in
  this plan.

Thus Plan 141 is **not a prerequisite for Plan 136 Stages 0-2**. If Plan 136
adds or changes a Loki-based alert before Plan 141 lands, that individual query
must be checked against both labeled and unlabeled current data.

### Plan 140 — service health contract

Plan 140 owns healthchecks, health metrics, alerts, and CI coverage for
liveness. It must not require every service to have a Promtail job: that would
reintroduce the high-volume ingestion mistake this plan exists to correct.
Plan 141 owns the separate source-policy registry and logging coverage test.

### Plan 134 — archiver endpoint failure contract

Plan 134 intentionally starts with a week of warning logs. Its observation
window should begin after Plan 141, or it must use a temporary query proven to
match current Archiver JSON labels. Otherwise a parsing/dashboard gap could be
mistaken for “no failures.” This dependency places Plan 141 before Plan 134 in
the build order even though Plan 134 has a slightly higher standalone score.

### Plans 139 and future host maintenance

Plan 139 Stages A+B still go first because they shorten every later CI cycle.
Plan 141 adds focused behavioral fixtures; it does not absorb Plan 139's broad
test-suite maintenance. The planned host-maintenance runbook should reuse Stage
4's no-replay and ingestion-health checks after any Promtail or Docker restart.

## Success criteria

1. Every newly retained Loki record has `service` and `source`.
2. Every retained record from a severity-bearing source has a normalized
   `level`, or is explicitly counted as unclassified by policy.
3. Airflow `WARNING|ERROR|CRITICAL` records and OAuth failures appear in Error /
   Warning Logs; INFO/control-plane noise and successful auth subrequests do not.
4. Active application files are tailed without treating rotated backups as new
   sources, and a Promtail recreation causes no replay.
5. Dashboard selectors and parser behavior share fixture-backed tests.
6. A 24-hour production soak shows zero contract violations and records a
   sustainable 90-day capacity forecast.

## Out of scope

- Storage caps and deletion policy — Plan 135.
- Container liveness and healthcheck coverage — Plan 140.
- Solver efficacy, metrics freshness, and automatic recycle — Plan 136.
- Changing application business-event verbosity.
- A wholesale migration from Promtail to Grafana Alloy. Promtail is end-of-life
  upstream, so migration deserves separate prioritization after this contract
  defines behavior that any collector must preserve.
