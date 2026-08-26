# Plan 141: Structured Log Ingestion and Dashboard Contract

## Status

**Stages 0-3 DEPLOYED to production 2026-08-25 19:52 UTC (PR #247, in the
combined evening deploy). Stage 4's production evidence was accepted on
2026-08-26 after an 80,658-second observation window and ten controlled source
probes. One closeout correction remains to deploy: the dashboard queried a
nonexistent Promtail counter name.**

**One acceptance instrument is known-unreliable.**
`scripts/verify_promtail_contract.py` produced a false "Promtail dropped it"
in CI on 2026-08-25 and passed on re-run of the identical commit; see Stage 4
below. Treat a contract failure during the soak as unproven until re-run.

Written 2026-08-18 while closing Plan 135 Stage 5. Production is stable and the
storage bounds are live; this plan is a correctness and usability follow-up,
not an incident response and not a reason to reopen Plan 135.

Priority **85 (high)**. Effort **S plus a 24-hour production soak**.

### Corrections applied after the PR #247 code review

Five findings, all on the Stages 0-3 commit, all fixed before deploy.

1. **The analytics snapshot broke at deploy.** Adding five metric names to
   `ANALYTICS_METRIC_COLUMNS` invalidated the snapshot already on disk, because
   `validate_snapshot` demanded exact set equality with `METRIC_NAMES`. The
   first restart would have read its own file as unsupported, set every
   `cartracker_*` analytics gauge to NaN and emptied `/info`'s `public_stats`
   until the next hourly refresh. The metric set is no longer part of the schema
   contract: a name the file omits publishes as `None`, and a name it carries
   that this release does not know is dropped, so forward and rollback deploys
   are symmetric. `SCHEMA_VERSION` stays 1 — the document shape did not change.
2. **`ct-log-error-spike` widened silently.** Its `{service=~".+"}` selector was
   tuned against six application streams that were the only ones carrying a
   `level` label. Giving Airflow and oauth2-proxy that label would have extended
   a >5-in-5m page to DAG-failure bursts and to every 5xx behind the auth proxy.
   The service set is now explicit and asserted equal to `APPLICATION_SERVICES`.
   Those stdout sources are visible on the dashboard from day one; putting them
   in an alert needs a threshold from Stage 4's measured volume.
3. **The contract-violation panel could not fire.** It counted `level=""` in
   Loki for services whose Promtail stages drop `level=""` — the malformed
   record it existed to surface never reaches Loki, so it read 0 and looked
   green. That count exists only in `logentry_dropped_lines_total`, and Promtail
   was not a Prometheus target at all. It is now scrape job `promtail`, inside
   `ct-service-down`'s job set, and the panel reads the drop counter. A second
   panel charts drops by reason, which is what success criterion 2 asks for.
4. **The Airflow severity regex was narrower than the text filter it replaced.**
   Requiring severity in the second or third whitespace field matched the
   structlog shape Stage 0 sampled and nothing else; the gunicorn supervisor's
   `[ts] [7] [CRITICAL] WORKER TIMEOUT` and the classic
   `[ts] {file.py:123} ERROR -` both fell through to the unclassified drop,
   where the old filter would have kept them. The pattern now takes the leftmost
   bracketed severity within a bounded prefix, the unbracketed severity after a
   `{file:line}` field, or a line-leading severity. Fixtures cover all three;
   the two added shapes are upstream defaults rather than lines observed in the
   sampled window, and the corpus records that distinction.
5. **`classify_line` was stricter than the pipeline it models.** It dropped an
   application record with no `logger`, which production retains, and let an
   OAuth access status win over a lifecycle severity, which Promtail's stage
   order gives to the lifecycle. Both are fixed, and both would have stayed
   invisible because nothing ran the real pipeline.
   [`scripts/verify_promtail_contract.py`](../../scripts/verify_promtail_contract.py)
   now replays every fixture through `grafana/promtail:3.5.8` with
   `-dry-run -stdin` and compares Go's labels against the corpus; it was
   confirmed to fail on both original divergences before they were fixed. The
   `promtail-config` CI job runs it, which satisfies Stage 3 item 4 by
   execution rather than by configuration parsing.

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
7. Let application records carry structured fields.
   [`shared/logging_setup.py`](../../shared/logging_setup.py) emits exactly
   `ts`, `level`, `logger`, `msg` and formats `record.getMessage()`, so any
   context a caller passes via `extra=` is **silently discarded**. Merge a
   bounded set of `extra=` fields into the emitted JSON, rejecting unbounded or
   prohibited keys at format time rather than trusting callers.

   The four existing keys keep their names and values, so this is additive:
   Promtail's `json` stages extract named keys, and nothing in the repository
   strict-parses the four-field shape. Prove it with a fixture pair — an
   old-shape and a new-shape record through the same stage — and change no
   dashboard selector in this stage.

   This item is here because [Plan 151](plan_151_distributed_tracing_and_runtime_topology_audit.md)
   defers trace/log correlation to "Plan 141's stable fields," and those fields
   cannot exist while the formatter drops them. Plan 142's coordination
   narration is the first concrete consumer.

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

### Stage 2a — Show the cooldown funnel from the durable event record

Added 2026-08-25 while selecting the new 403 early-warning threshold. The rate
alert answers whether the current batch is unhealthy, but it does not show how
many detail scrapes are progressing into deeper exponential-backoff buckets.
That is an observability question, not another paging condition.

1. Build an hourly mart from
   `ops_normalized/blocked_cooldown_events/**/*.parquet`, reached through
   `stg_blocked_cooldown_events`. Count each `blocked` or `incremented`
   transition once and exclude `cleared` lifecycle events.
2. Use the same stable attempt buckets as `mart_cooldown_cohorts`: `1`, `2`,
   `3-4`, `5-10`, and `11+`. This measures flow through the cooldown funnel;
   the existing cohort mart continues to measure the current backlog.
3. Publish one rolling seven-day gauge per bucket through the durable
   `dbt_runner` analytics snapshot. The window is the latest 168 hourly buckets,
   so a dashboard point reads as “X scrapes landed in this bucket over the last
   seven days.”
4. Add a single Pipeline Health time-series view for all five gauges. Do not add
   thresholds or an alert rule; the purpose is trend and funnel visibility.

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
7. Confirm the Airflow severity shapes against real control-plane output. The
   sampled seven-day window contained no ERROR or CRITICAL at all, so the
   fixtures for the shapes the parser most needs to catch are upstream defaults
   rather than observed lines. Tail each of the three containers and check that
   nothing carrying a severity is landing in
   `logentry_dropped_lines_total{reason="airflow_unclassified_control_plane"}`;
   that counter is now scraped, so this is a query rather than a log read.
8. Decide from the measured stdout volume whether Airflow and oauth2-proxy
   belong in `ct-log-error-spike`, and at what threshold. They were deliberately
   left out rather than inherited — see correction 2.

Do not rewrite or selectively delete the misclassified historical Loki data.
Let the 90-day retention policy age it out unless capacity evidence creates a
separate, explicitly approved deletion need.

#### Stage 4 deployed 2026-08-25 19:52 UTC — soak clock starts here, not at merge

Stages 0-3 reached production in the 2026-08-25 evening deploy. `promtail.yml`
was applied by `redeploy.sh --restart`, which verified inode 519901 matches the
file on disk — **not** by `docker kill -s HUP`, which reloads the stale
unlinked inode and logs success while doing it.

> **The soak clock starts at the deploy, not at the merge.** CAR-10 was marked
> "Soaking" when PR #247 merged, but production ran PR #241 until 19:52, so
> that window measured code that was not running anywhere. Same error affected
> Plan 142 Stage 1 the same evening. **A status marker is not a deploy**, and
> this plan's acceptance runs 24 hours from 2026-08-25 19:52 UTC.

#### Stage 4 closeout evidence — accepted 2026-08-26

The user accepted the production observation at 18:16 UTC, after 80,658
seconds (22h24m) from the 2026-08-25 19:52 deploy. The shortened interval is
recorded rather than rounded up to 24 hours.

| Acceptance read | Result |
|---|---|
| `ct-403-log-spike` | `Normal`, health `ok`, and zero alert annotations during the observation window. The old noise remained demonstrable — `shared.minio` produced 403 text matches — but could not drive the metric-only rule. The authoritative counters recorded about 36 exact 403 outcomes among 25,518 detail fetches (0.14%). |
| Required labels | Zero retained records missing `source`; zero Airflow or oauth2-proxy records missing expected `level`. |
| Excluded noise | Zero retained Airflow `DEBUG`/`INFO` records and zero retained successful OAuth `/oauth2/auth` 202 subrequests. |
| Promtail health | Zero restarts and zero discovery, parsing, out-of-order, too-old, or empty-label errors after deploy. |
| Airflow severity | The deployed parser matched 186 scheduler warnings and one DAG-processor warning in raw stdout. No generic severity-bearing line fell through the same parser as unclassified; the apiserver emitted no natural actionable severity in the window. |
| Controlled source proof | Ten uniquely tagged warnings were produced: six application-file services, all three selected Airflow stdout services, and one OAuth 401. Loki returned each exactly once with the expected `service`, `source`, and normalized `level=WARNING`; the Error / Warning selector returned all ten. |
| Loki line volume | 66,998,820 bytes over 80,658 seconds, or **71.77 MB/day**, down **72.1%** from Stage 0's 257.34 MB/day. The straight-line 90-day projection is **6.46 GB (6.02 GiB)** versus 23.16 GB at Stage 0. |
| Physical capacity | Loki occupied 4,120,786,946 bytes (4.12 GB); `/mnt/data` had 132,460,101,632 bytes (132.46 GB) available. |
| `ct-log-error-spike` scope | Keep Airflow and oauth2-proxy excluded. The retained window contained 194 Airflow warnings and 290 OAuth warnings but zero `ERROR` or `CRITICAL` records from either source. There is no observed error distribution from which to justify a paging threshold; dashboard visibility remains the correct contract. |

The closeout cross-check found one correction that must deploy before the plan
can archive. Production Promtail 3.5.8 exports
`logentry_dropped_lines_total`, while the dashboard and its tests named the
nonexistent `promtail_dropped_lines_total`. The panel's `or vector(0)` therefore
turned the instrumentation error into a healthy-looking zero. Commit
`1d96980` corrects both panels, their regression assertions, and this plan's
counter references. Until that commit is deployed and the live dashboard query
returns the real series, Plan 141 remains in the build order rather than the
completed archive.

#### The contract checker itself is unreliable — found 2026-08-25

`scripts/verify_promtail_contract.py` **produces false contract violations**,
which matters because it is one of this stage's acceptance instruments.

Observed in CI on 2026-08-25: `airflow_warning: corpus says retained, Promtail
dropped it`, on a commit touching nothing in `promtail/`, the fixture corpus,
or the script. The identical commit passed on re-run, and the checker passed
10/10 locally.

The mechanism is in `_run()`: every line for a `(service, source_type)` pair is
fed through **one** `promtail -dry-run -stdin` invocation, and the result is
matched **by line text**, so any line missing from stdout is scored as
*dropped*. If Promtail exits before flushing, a retained line reads as a
contract violation. `airflow-scheduler/container_stdout` batches four lines and
exactly one went missing.

**A false "Promtail dropped it" during the Stage 4 soak is indistinguishable
from a real contract violation**, which is the failure mode this plan exists to
eliminate. Owned by
[Plan 139](plan_139_test_suite_maintenance.md) **Stage G** rather than fixed
here: this plan owns what the log contract says, and that one owns whether the
instrument checking it can be believed.

#### The runbook's own 403 check reproduces this plan's founding defect

While reading Plan 136's solver outcomes on 2026-08-25, the runbook's
`docker logs … | grep -ioE 'solved|403'` reported **12 `403`s** over a window
in which the authoritative counter `cartracker_detail_fetch_total` reported
**`403=0`**.

That is `ct-403-log-spike`'s bug — an unanchored `403` match catching lines that
are not 403 responses — surviving in the operator runbook after being fixed in
the alert rule. **Corrected in
[`runbook_solver_oom_and_recycle.md`](../runbooks/runbook_solver_oom_and_recycle.md)
to read the counter.** Worth noting as evidence that this plan's scope is the
pattern, not the single rule: anywhere a bare `403` is grepped out of logs is
suspect.

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

- Revalidate `ct-403-log-spike` after Stage 1. Plan 141 keeps its legacy UID but
  removes its Loki-text dependency: it is an early-warning Prometheus ratio for
  more than 5% exact `outcome="403"` results among at least 25 detail fetches in
  five minutes. `ct-detail-fetch-failing` remains the separate 100%-failure
  detector over twenty minutes.
- The cooldown funnel is the durable downstream view of those outcomes. It is
  built from normalized cooldown event Parquet rather than Loki text or current
  backlog snapshots, and deliberately has no alert attached.
- Plan 136 warning-only observations should name the expected `service` and
  `level` contract rather than use unconstrained text queries.
- The short Airflow HMAC-key warning discovered during this audit is a real
  configuration finding, not log noise. **Its remediation belongs to
  [Plan 142](plan_142_planned_host_maintenance.md) Stage 0** — corrected
  2026-08-23, because this bullet and Plan 136's twin both routed it to Plan
  136 Stage 0, which had completed on 2026-08-18. Two plans pointing at a
  closed stage is why it went unowned. **What this plan owns is the line, not
  the fix:** freeze it as a Stage 0 fixture and prove in Stage 1 that it
  reaches Error/Warning Logs with `service` and `level` set. Do not suppress
  it, and note the shape of that temptation — a plan that both classifies
  noise and silences its sources can satisfy criterion 3 by deleting the
  warning instead of labelling it.
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
7. Application records can carry bounded structured fields via `extra=`, with
   the four existing JSON keys unchanged for records that pass none.
8. The Pipeline Health dashboard charts rolling seven-day transition counts for
   cooldown attempt buckets `1`, `2`, `3-4`, `5-10`, and `11+`, sourced from the
   durable cooldown event Parquet record and used for observability only.

## Out of scope

- Storage caps and deletion policy — Plan 135.
- Container liveness and healthcheck coverage — Plan 140.
- Broader solver efficacy, metrics freshness, and automatic recycle — Plan 136;
  the exact 403 early-warning ratio and cooldown funnel above are the narrow
  Plan 141 overlap.
- Changing application business-event verbosity.
- A wholesale migration from Promtail to Grafana Alloy. Promtail is end-of-life
  upstream, so migration deserves separate prioritization after this contract
  defines behavior that any collector must preserve.
