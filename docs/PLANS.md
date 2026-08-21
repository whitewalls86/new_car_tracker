# Cartracker - Plans & Roadmap

Each plan has its own file in `docs/`. This file is the index only. For system
design patterns, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Current State (as of 2026-08-20)

Site is live at https://cartracker.info. All major pre-lakehouse foundations
are complete: auth, data migration, CI/CD, integration testing, MinIO artifact
store, processing service, Airflow migration, Grafana, dashboard restructure,
full decommission, storage normalization, and adaptive-refresh feature
foundation.

Airflow owns scraping and maintenance. n8n is fully removed. Postgres owns hot
operational state. MinIO stores bronze HTML and analytical history. dbt
currently runs on DuckDB against MinIO silver, but DuckDB is now considered a
transition analytics endpoint rather than the future platform target.

**Now:** Plan 120's final Gate F production verification is complete, and Plan
139 Stages A+B have taken the CI path every plan below pays from 333s to a
stable ~260s, with coverage now reported on every run. Plan 136 Stage 0 shipped
and was **verified against production** on 2026-08-18 — Airflow itself reports
the 20+20 pool, the shared anchor did not leak it to the other three services,
and `ct-pipeline-failures` now renders exactly two named instances where it
previously rendered a third reading `DAG [no value] failed`. Plan 140 Stage 3 is
verified, Stage 1's 24-hour soak closed clean, and **Stage 2 deployed to
production on 2026-08-20** (PRs #221 and #222). `cartracker_container_health`
now publishes three states for all 28 running services — 27 healthy and one
`-1` for `oauth2-proxy`, the documented distroless exception — from a dedicated
exporter computing at scrape time behind `docker-socket-proxy`, and
`ct-service-down` went from covering two of eight scrape jobs to all nine under
an exact-set-equality test. A Service Health row opens the Infrastructure
dashboard. **Its 24-hour soak is the one gate still open**, and it exists
because the first deployed expression produced a false page in six minutes: a
filtering comparison dropped the series when a container recovered, and
Grafana's `reduce: last` kept the dead value alive for the rest of its 600s
window. `== bool` fixed it, but the soak is what proves twenty-eight new alert
instances stay quiet. **Plan 143 is complete as of 2026-08-20.** PR #217 (merge
`e5d3a46`) shipped the saved-SQL, post-build snapshot that makes `dbt_runner` the
direct metrics owner while `ops` renders `/info` without opening DuckDB or using
the transient artifact queue as freshness; PR #218 (merge `a3cdd59`) corrected
the unscoped Grafana consumers and the 900-second threshold that the first soak
exposed. The corrected soak recorded 24 hourly publications, no failed publish,
a 3,580.9s worst-case freshness gap against the 4,500s threshold, zero DuckDB
lock conflicts, and `/info` at 0.157s.
**Plan 136 Stage 2 deployed to production on 2026-08-20** (PR #223, merge
`50bba68`) — the solver-outcome counters, which are the layer no healthcheck can
supply, because the solver was healthy for all eight hours. Prometheus now
scrapes the scraper at all, which it never did before; all six outcome series
published at `0` before any traffic, both new alert expressions returned exactly
one series each, and `ct-container-unhealthy` stayed Normal across all 28
instances through three container recreates — an unplanned live test of Plan
140's `== bool` fix. The 21:00 build then closed D1's partial-hour defect for
real — `data_through` reads 20:00 rather than 21:00, and the hourly observation
count is 8,133 where the unfixed query would have published about one minute of
data against a threshold of 100. **Its 24-hour soak runs to 2026-08-21 ~20:42
UTC.** Two corrections came out of building it. The alert expression
the plan specified was the same filtering-comparison shape that false-paged Plan
140 six minutes after its deploy, so both new rules are written as `bool`
products. And one rule cannot cover both solver failure modes: a *refusing*
solver never caches credentials and so drives solver request volume **up**,
while a *lying* solver caches normally for 25 minutes and moves that counter not
at all — only its 403s on detail fetches show it. Hence
`ct-solver-not-solving` (fast, solver counter) and `ct-detail-fetch-failing`
(shape-independent, fetch counter). Stage 2 also closed D1's remaining half:
the Plan 143 snapshot selected `MAX(hour)` from an hourly mart while the build
runs at `0 * * * *`, so it published the in-progress hour as an hourly total —
a manufactured drop under a threshold of 100, and the likely reason that alert
fired forty minutes into the healthy recovery.
**Stage 3 was deliberately left for a second change**: its recycle interval is
chosen from what these counters show, and it needs a `POST` verb on the socket
proxy that Plan 140 left read-only. **That is what takes Plan 136 out of the
executable rows for now** — its next slice is an observation window, not code,
so the top of the build order moves down to the rows that can be worked today.
Only after the counters have a baseline does
Stage 4 get restart authority. It inherits the
`docker-socket-proxy` Plan 140 Stage 2 introduced, so that authority is a
single added verb on an existing grant rather than a second socket path. Plan 142 then
turns the same drain and health primitives into a safe, explicit whole-host
maintenance workflow. Plan 141 then makes the newly bounded log pipeline and
its dashboards share one tested schema before Plan 134 begins its warning-log
observation window. **Plan 133 deployed and verified on 2026-08-20** — 720
artifacts read through the pack path across April-July with 0 failures — so the
next data-integrity step is Plan 132, now unblocked.
Plan 138 should land before the next major platform milestone, consuming rather
than recreating Plan 143's public-stats cache. Plan 125 then resumes at its
remaining Gate C production measurement and Gate D reader migration -- not at
the already-proven early gates; Gate D swaps Plan 143's producer adapter rather
than rebuilding its page and metric contracts.

**Two incidents in four days (2026-08-14 solver, 2026-08-18 Airflow apiserver)
were each found by a human noticing downstream damage, not by an alert.** That
is why observability work occupies the top of this order. Plan 136 covers the two
components that actually failed; Plan 140 covers the other twenty-four before
they do.

Plan 112 remains intentionally paused until Plan 125 supplies stable
Iceberg-native inputs. Plans 114, 115, and 128 have completed their intended
work and no longer belong in the executable queue. Plans 124 and 131 closed out
2026-08-18, as did Plan 120's final Gate F production verification. Plan 129
is a production system under rollout rather than a new build.

---

## Coordinating Roadmap

| Plan | Title | Status |
|------|-------|--------|
| [117](plan_117_storage_and_adaptive_refresh_roadmap.md) | Open lakehouse + adaptive refresh roadmap | Draft |

---

## How priority and effort work

The **build order is authoritative** when scores are close: it includes
dependencies, safe stopping points, and the cost of switching between systems.
The **priority score** is a 0-100 planning aid, not a promise of delivery date:

- **90-100 -- critical:** prevents current production or data-integrity loss.
- **75-89 -- high:** removes a known defect, unlocks another plan, or closes a
  time-sensitive public/operational gap.
- **55-74 -- medium:** strategic platform work with no immediate incident.
- **Below 55 -- conditional:** worthwhile only after its trigger or dependency.

Scores combine production impact, urgency, data-loss risk, dependency leverage,
and readiness. Effort is relative engineering scope including tests and deploy
evidence: **XS** (hours to 1 day), **S** (2-4 days), **M** (1-2 weeks), **L**
(2-4 weeks), and **XL** (multi-phase or more than 4 weeks). A required soak or
observation window is written separately and does not inflate coding effort.

## Current closeout -- finish before opening another large build

Plan 120's authenticated production download and checksum round trip was
verified on 2026-08-18. **Plan 140 Stage 1 and Plan 143 Stage 5 both closed
green on 2026-08-20** and have left this table; their evidence is recorded in
their plan documents. Plan 140 Stage 2 replaces them, having deployed the same
day. **Plan 136 Stage 2 deployed on 2026-08-20** and adds the two rows below it.
Its D1 half already closed green at the 21:00 build the same evening. What
remains here is **verification, not work**: no code is owed on these rows. Two
Plan 135 gates ride weekly schedules and land on the same Sunday; the two
24-hour soaks land within two hours of each other on 2026-08-21.

This table is now the reason the build order's top row is not workable today —
see [What to work while these soak](#what-to-work-while-these-soak) below.

| Plan | Check | Lands | What it proves |
|---|---|---|---|
| [136](plan_136_solver_recycle_and_liveness.md) Stage 2 | `ct-solver-not-solving` and `ct-detail-fetch-failing` stay inactive across 24 hours, and the counters show a solve-rate **shape** | 2026-08-21, 24h from the 20:42 UTC deploy | Two things, and the second is what unblocks work. First that the `bool`-product form is quiet -- both rules returned exactly one series at 0 on their first evaluation, which is the property the filtering form lacks. Second that the counters answer open question 2: whether the solve rate decays gradually or falls off a cliff is what chooses Stage 3's recycle interval, and the plan says not to pick one before reading it. The **D1 half of this row closed green at the 21:00 build** and has moved into the plan document: `data_through` 20:00 rather than 21:00, 8,133 observations for a complete hour, snapshot published in 0.130s. First live counters at 21:02 read 457 detail fetches against 1 solver bootstrap -- a 457:1 ratio that confirms the solver counter is legitimately low-volume when healthy, and retroactively justifies splitting one rule into two |
| [140](plan_140_service_health_contract.md) Stage 2 | `cartracker_container_health` produces **zero false pages** across a 24-hour window, and `ct-container-unhealthy` stays inactive with all 28 instances `Normal` | 2026-08-21, 24h from the 19:04 UTC correction | That twenty-eight new alert instances are trustworthy. This soak is not a formality: the **first deployed expression false-paged within six minutes**, because a filtering comparison drops a recovered container's series and `reduce: last` then keeps the dead value alive for the rest of its 600s window -- `flaresolverr` stayed firing for eleven minutes after recovering. `== bool` fixed that, and a full day is what proves nothing else of that shape is left. Expect exactly one standing alert: `oauth2-proxy` at `-1`, on the daily coverage cadence, which is the designed behaviour and not a false page. **Partial evidence already in:** the Plan 136 Stage 2 deploy recreated `scraper`, `processing` and `dbt_runner` at 20:42 UTC and all 28 instances stayed `Normal` — an unplanned live rerun of the exact scenario that produced the false page, since it was a container restart that triggered it |
| [135](plan_135_storage_observability.md) criterion 5 | `cartracker_parquet_data` publishes a real series | 2026-08-23, `disk_usage` slow-tier walk at 04:00 UTC | That the per-path panel can answer *"what is filling `/mnt/data`?"* -- the question this plan was written about. The MinIO volume is the majority of that disk's 59 GiB and has still never completed a walk, so the panel is currently silent on the bulk of it. Confirmed still 0 series on 2026-08-20, exactly as designed before the first walk |
| [135](plan_135_storage_observability.md) Stage 5 | `prune_task_logs` completes its first scheduled run | 2026-08-23, `17 4 * * 0` | That Airflow's 30-day task-log retention is **enforced** rather than merely configured. The run reports run directories examined and deleted; `cartracker_airflow_logs` was 1.2M inodes and 87% of a 456s walk, so this is also what lets that volume move back to the daily tier |

None blocks implementation work. Record each result in its plan document, then
remove its row from this closeout table once it is green.

### What to work while these soak

**Plan 144 was the recommendation here and closed 2026-08-21.** It shipped as
PRs #224 and #225 and was verified against the live fleet the same day, so this
section's argument is settled rather than pending; the reasoning is kept below
because it is the record of a deliberate deviation from the build order.

**Next: Plan 142 Stage 0**, which was the "once the soaks close" half of the
same recommendation and is now the top workable row.

The deviation was argued, not assumed:

1. **Plan 144 was XS and this section was bounded by two soaks landing on
   2026-08-21.** Plan 142 is M *plus a first observed maintenance window*.
   Starting it then meant opening a large build against the closeout table's
   own instruction, and stopping it mid-flight to read soak results.
2. **Plan 142 Stage 2 consumes `redeploy.sh` rather than forking it**, which
   the build-order row already said. Hardening it first was dependency order,
   not queue-jumping.
3. **It gained a fourth piece of evidence, from the Plan 136 Stage 2 deploy —
   and the sharpest kind: the script was not used.** That deploy was driven by
   hand, because `redeploy.sh` would have run `docker compose up -d` without
   `--no-deps` across `scraper processing dbt_runner`, and would have reported
   "Done." after `sleep 10` with no idea whether anything was healthy. A deploy
   tool that the operator routes around on a real deploy is the argument for
   fixing it.
4. **The single-file bind-mount trap was reproduced twice.** Plan 140 found it
   on 2026-08-20 (`prometheus.yml` mounted as a *file*, so `git pull` lands the
   new content on a new inode while the container keeps reading the old one,
   and SIGHUP logs a successful reload of the stale config). The Plan 136
   deploy hit it again hours later — host `519823`, container `519700`.

**How it turned out.** Two more defects surfaced during the build, both the
same shape as the first four: a recreate silently orphans peers holding a
cached address (Plan 136 D6), and `up -d` on an unchanged service reports a
successful deploy having done nothing. The second was found by dry-run *after*
the first five had shipped. The plan grew from three defects to six without
growing past XS.

Plan 141 remains the other genuinely unblocked row and is the right pick if a
larger slice is wanted instead; its live `ct-403-log-spike` false positive is
still the best fixture source available. Note it fires *diurnally* — the
2026-08-20 20:47 UTC rule dump had it inactive, with the reported noise falling
between 03:00 and 08:00 UTC — so capture fixtures across that window rather
than concluding from a single quiet reading that the defect went away.

## Default build order

This is the default single-maintainer sequence. Do not start a lower row merely
because it is smaller while a higher row has an executable next step.

| Order | Plan | Title | Next executable slice | Priority | Effort | Depends on / safe stopping point |
|---:|---|---|---|---:|---|---|
| 1 | [136](plan_136_solver_recycle_and_liveness.md) | Solver recycle and real liveness | **Stage 2 deployed 2026-08-20 (PR #223).** Next slice is an *observation window, not code*: read the outcome rates for a week, then choose Stage 3's recycle interval | 98 | M | Stage 0 verified; analytics freshness moved to Plan 143; 0b moved into Plan 140 Stage 2. **Not workable today** — Stage 3 needs both the counter baseline and a `POST` verb on the socket proxy; soak counters before Stage 4 auto-restart |
| 2 | [142](plan_142_planned_host_maintenance.md) | Planned host maintenance and production quiescence | Freeze the successful Ubuntu-update window as fixtures, then build separate maintenance intent, truthful drain status, and the checked-in host procedure | 86 | M + first observed window | Reuse Plan 136 drain semantics; final resume gate requires soaked Plan 140 health coverage |
| 3 | [141](plan_141_structured_log_ingestion_contract.md) | Structured log ingestion and dashboard contract | Freeze production-derived fixtures and baseline, then align parsing, labels, filters, and dashboard selectors; fix `ct-403-log-spike` as the first case | 85 | S + 24h soak | Does not block Plan 136; should precede Plan 134's warning-log observation window. Has a live false-positive to work from: see below |
| 4 | [140](plan_140_service_health_contract.md) **Stage 4** | Retire DAG sensors as the health signal | Demote `http_health_sensor` from notifier to gate, now that a stopped container pages on its own | 70 | XS | **Gated on the Stage 2 soak**, which is in the closeout table above. Stages 1-3 are deployed and verified. The `flaresolverr` fire test already showed the alert going Pending inside a minute, far ahead of any DAG run, so this is a demotion decision rather than new signal work � do not remove the sensors, they remain load-bearing for DAG correctness |
| 5 | [134](plan_134_archiver_endpoint_failure_contract.md) | Archiver endpoint failure contract | Add warning-only failure predicates and begin the one-week observation window | 88 | S | Plan 141 first; one-week soak before enforcement; pause if real failures need repair |
| 6 | [132](plan_132_unrecorded_artifact_recovery.md) | Recover unrecorded bronze artifacts | Run Stage 0 gates, build the manifest, then reparse a bounded cohort | 91 | L | **Unblocked 2026-08-20** — Plan 133 deployed and verified. No destructive action in the audit stages. Stage 2's reparse is also where defect 2's cache fix gets its first real measurement |
| 7 | [138](plan_138_public_surface_refresh.md) | Public surface refresh | Truth pass, public-root contract, accessible assets, Plan 143 stats presentation, and project-updates snapshot | 84 | L | Plan 143 supplies the stats contract; land before the next major platform milestone |
| 8 | [125](plan_125_duckdb_to_iceberg_migration.md) | DuckDB-to-Iceberg analytics migration | Gate C production runtime measurement, then Gate D reader inventory/dual-run | 81 | XL | Plan 120 closeout; swap Plan 143's producer adapter while preserving its snapshot and metric contracts |
| 9 | [112](plan_112_refresh_policy_backtesting.md) | Adaptive-refresh backtesting | Resume policy backtest/model gates on pinned Iceberg snapshots | 76 | L | Plan 125 stable Iceberg-native inputs |
| 10 | [113](plan_113_production_adaptive_refresh.md) | Production adaptive refresh | Promote one reviewed, pinned policy into ops claim logic | 74 | M | Approved Plan 112 result; no live model dependency |
| 11 | [137](plan_137_legacy_bronze_parquet_disposition.md) | Legacy bronze Parquet disposition | Codify the read-only baseline and row-complete disposition manifest | 72 | XL | Reuse Plan 132 provenance/backfill safety; deletion remains separately approved |
| 12 | [69](plan_69_terraform.md) | Terraform IaC | `terraform import` the existing VM/network/firewall until `plan` shows no diff against production | 66 | M | **Moved out of the backlog 2026-08-20** — its trigger is "a second environment is approved", and Plan 121 is that environment. Must land before 121, not after |
| 13 | [121](plan_121_staging_environment.md) | Staging environment | Stand up the smallest fixture-backed deployed environment, provisioned from Plan 69's modules | 63 | L | Plan 69 first, so staging and prod come from one module set instead of two hand-built hosts. Prefer after Plan 125 reader shape settles unless needed earlier for risky rollout |
| 14 | [139](plan_139_test_suite_maintenance.md) **C** | Profile the 92s `tests/integration/dbt/` step | Run it with `--durations=20` in CI and record the per-test breakdown before proposing any change | 60 | S | Measurement only; CI-only work — do not pip-install dbt locally |
| 15 | [119](plan_119_lakehouse_governance.md) | Lakehouse governance | Add measured catalog controls and auditability | 58 | L | Stable Plan 125 catalog and reader contracts |
| 16 | [139](plan_139_test_suite_maintenance.md) **D** | Intent markers and the coverage-gate decision | Move `report_dbt_run_results.py` into `dbt_runner/`, add the `oneoff` marker per test class, decide the gate and the `airflow/dags`+`dashboard` exclusion in writing | 52 | S | Several weeks of Stage A coverage data; opportunistic filler |
| 17 | [126](plan_126_basic_event_streaming.md) | Basic event streaming | Prove transport, replay, and one low-risk consumer | 49 | XL | Plans 125 and 112/113 clarify stable event semantics |
| 18 | [127](plan_127_streaming_adaptive_scrape_control.md) | Streaming adaptive scrape control | Add closed-loop control behind batch-parity and rollback gates | 42 | XL | Plan 126 plus approved Plan 112/113 behavior |

**Plan [139](plan_139_test_suite_maintenance.md) occupies two rows rather than
one**, because scoring it as a single plan (62) hid that its first two stages
were two lines of YAML and a dev dependency. That argument paid off: **Stages
A+B shipped 2026-08-18** (PR #213) and left the build order. The critical path
is now a stable ~260s against a 333s baseline, and coverage reports on every run
at 88% with no gate. Only the scheduling edge earned it — the pip-cache
hypothesis was wrong and was reverted under Stage B's own verification rule.
Stages C and D never had that argument and take their turn by score.

The step that previously looked urgent — covering the 25%-covered analytics
gauge module — first moved into Plan 136 Stage 1 and now belongs to **Plan 143**
with the full producer redesign. It therefore appears in no Plan 139 row. Tests
written against the old silent-stale behavior would encode exactly what Plan
143 deletes, so they belong with the replacement producer and snapshot contract.

Plan 136 Stage 4 is deliberately not part of the first slice: it grants restart
authority and should start only after Stage 2's outcome counters have established
a trustworthy baseline. Plan 134's observation window may run while Plan 136
proceeds, but the endpoint-by-endpoint 500 rollout returns to this order when
the evidence is ready.

**Plan 140 led despite scoring 87 against Plan 136's 98, and that argument is
now settled by delivery.** Plan 136's Stage 0b was to build a container-health
metric and Plan 140 Stage 2 to generalize it — one set of files, one mental
model — but reading 140 against the compose file settled it more sharply:
**Docker reports no health status at all for a container without a
healthcheck**, and only 7 of 31 services had one. A metric built at Stage 0b
would have been blank for the other 24, and a service with no healthcheck would
have been indistinguishable from a healthy one — 140's own words, that it
"would have caught the apiserver incident and missed the solver incident."

So 0b was never sequenced before 140; it **was** 140 Stage 2, and it landed
strictly better there: three states rather than two, with `-1` making "no
healthcheck configured" loud instead of absent. Stage 1's healthchecks shipped
first, so the metric covered 27 of 28 services on the day it deployed, and the
one it does not cover is *visible* rather than missing.

**Plan 136 now takes the top row**, with only Plan 140's soak and its gated
Stage 4 outstanding. It also inherits `docker-socket-proxy` — Stage 2
deliberately introduced the shape Plan 136 Stage 4 extends, so restart authority
becomes one added verb on an existing read-only grant rather than a second
socket path. Two smaller inheritances are worth naming because they were paid
for by 140's deploy rather than assumed: `ct-service-down` now actually covers
every scrape job, so any new exporter's liveness is watched by default; and the
`== bool` lesson applies directly to Plan 136's own solver-outcome alerts, which
are exactly the appear-and-disappear shape that false-paged here.

**Plan 143 left the executable order after its 2026-08-18 production
deployment and completed on 2026-08-20.** It preceded Plan 136's remaining Stage
2 despite scoring 94 against 98 because shipping the gauge work locally inside
Plan 136 would have created a serving boundary Plans 125 and 138 would both
replace. Plan 136 returns to scraper-owned solver telemetry without carrying
analytics-serving code, and Plans 125 Gate D and 138 now have a stable serving
contract to consume rather than rebuild.

**Plan 142 follows Plan 136 despite scoring 86 against Plan 141's 85.** This is
dependency order, not an emergency ranking. Plan 140 has now supplied the
resume gate — `cartracker_container_health` is the signal 142 resumes against,
pending its soak — and Plan 136 Stage 3 establishes the drain-aware
safe-boundary pattern Plan 142 should reuse. Once those exist, host maintenance is recurring
production safety work with a fully observed first-window failure record, so it
belongs ahead of dashboard-contract cleanup. It remains below Plan 136 because
there is no current unpatched emergency and no application endpoint should gain
host package or reboot authority.

**Plan 141 now has a live false positive to build its fixtures from.** The
2026-08-20 watch-item check found `ct-403-log-spike` firing — the only rule not
inactive — and notifying repeatedly between 03:00 and 08:00 UTC. Its expression
is `count_over_time({service="scraper"} |= "403" [5m]) > 10`, a bare substring
match against the whole JSON log line. It matches UUIDs and sha256 prefixes
containing the digits `403` (`...ded709847403`, `a1fd40307748`), so Loki returns
roughly 341 matching lines per hour drawn from ordinary INFO `write_html` and
`scrape_detail_fetch` traffic. Actual 403 responses over the same period: **23
in 12 hours**, against a healthy pipeline at 99.6% extraction yield and 2 block
events per hour.

This is precisely the defect Plan 141 exists to fix, and it argues for that
plan's parsing work over its dashboard work: the logs are **already** structured
JSON carrying a `level` field, so the alert should parse and match a real status
field rather than grepping the raw line. It is also a reminder that alert noise
is not only annoying — an operator who learns to ignore `ct-403-log-spike` is
being trained to ignore the scraper's alert channel, which is the channel the
2026-08-14 solver outage needed.

## Operational watch list and completed implementation awaiting closeout

| Plan | State | Attention required |
|---|---|---|
| [114](plan_114_sectioned_html_artifact_audit.md) | Audit complete; sectioned storage rejected | Record as completed; its findings feed Plans 129-131 |
| [115](plan_115_detail_unenriched_circuit_breaker.md) | Production bugfix implemented | Record deployment/verification and move to completed |
| [128](plan_128_false_block_detection.md) | Phases 1-4 implemented; no historical repair chosen | Record final verification and move to completed |
| [129](plan_129_zstd_dictionary_compression.md) | Dictionary v1 in production; backfill/lifecycle monitoring | Watch metrics; no new design work unless the run deviates |
| [131](plan_131_packed_cold_storage.md) | **Complete** — April-July packed and pruned, Stage 5 lifecycle DAG running on schedule | Monitor only; no new design work |
| [135](plan_135_storage_observability.md) | **Complete 2026-08-18** — both disks visible, alerts proven, all log stores bounded, maintenance runbook live | Monitor scheduled storage and task-log maintenance; parsing/dashboard follow-up is Plan 141 |

## Paused or blocked

| Plan | Priority | Effort | Resume when |
|---|---:|---|---|
| [130](plan_130_parser_input_projection.md) | 45 | L | Plan 129 reversible options are exhausted and the parser taxonomy gap is closed |
| [114](plan_114_sectioned_html_artifact_audit.md) follow-on | 30 | L | New evidence overturns the measured negative storage result |

## Plan inventory

| Plan | Title | Status |
|------|-------|--------|
| [112](plan_112_refresh_policy_backtesting.md) | Iceberg + MLflow adaptive refresh backtesting | Paused after foundation proof |
| [113](plan_113_production_adaptive_refresh.md) | Production adaptive refresh integration | Draft |
| [114](plan_114_sectioned_html_artifact_audit.md) | Sectioned HTML artifact audit | Complete - sectioned storage rejected on measured results |
| [115](plan_115_detail_unenriched_circuit_breaker.md) | Detail unenriched circuit breaker | Implemented - closeout record pending |
| [119](plan_119_lakehouse_governance.md) | Lakehouse governance + catalog expansion | Draft |
| [120](plan_120_ci_lake_snapshot_delivery.md) | CI + local lake snapshot delivery | **Complete 2026-08-18** — Gate F production download and checksum round trip verified |
| [121](plan_121_staging_environment.md) | Staging environment | Draft |
| [124](plan_124_trawl_memory_guardrails.md) | Trawl browser solver memory guardrails | **Complete** - verified in production 2026-08-18; zero host-wide OOM since deployment |
| [125](plan_125_duckdb_to_iceberg_migration.md) | DuckDB to Iceberg analytics migration | Gates 0.5, 0, A, and B complete; Gate C measurement next |
| [126](plan_126_basic_event_streaming.md) | Basic event streaming foundation | Draft / future |
| [127](plan_127_streaming_adaptive_scrape_control.md) | Streaming adaptive scrape control | Draft / future |
| [128](plan_128_false_block_detection.md) | Cloudflare challenge pages swallowed as successful detail scrapes | Implemented through Phase 4 - closeout record pending |
| [129](plan_129_zstd_dictionary_compression.md) | Trained zstd dictionary compression for bronze HTML | In production — dict v1 live, backfill running |
| [130](plan_130_parser_input_projection.md) | Parser-input projection (truncating raw HTML) | Draft — blocked on 129 + taxonomy gap |
| [131](plan_131_packed_cold_storage.md) | Packed cold storage for bronze HTML | **Complete 2026-08-18** — April-July packed and pruned: 3.61M objects → 288, 0 refused; Stage 5 lifecycle DAG green on its first scheduled run |
| [132](plan_132_unrecorded_artifact_recovery.md) | Recovering unrecorded bronze artifacts | Draft — Stage 0 gate not run |
| [133](plan_133_pack_read_path_hardening.md) | Pack read path hardening | **Complete 2026-08-20** — PR #219 (`5066bc1`) deployed to `ops`, `archiver`, `pack-worker`, and `processing`; post-deploy verification read 720 artifacts through the pack path across April-July with 0 failures. Unblocks Plan 132 Stage 2 |
| [134](plan_134_archiver_endpoint_failure_contract.md) | Archiver endpoint failure contract | Draft — measurement-first rollout not started |
| [135](plan_135_storage_observability.md) | Storage observability | **Complete 2026-08-18** — both disks visible, alerts proven, all log stores bounded, runbook live, `df /` 79% → 51%; criterion 5's MinIO half publishes on the first Sunday slow-tier walk (2026-08-23) |
| [136](plan_136_solver_recycle_and_liveness.md) | Solver recycle + real liveness detection | **Stages 0 and 2 deployed and verified in production (2026-08-18, 2026-08-20); Stage 1 transferred to Plan 143 before deployment** — prototype commit `584f100` established the fail-loud contract but not the accepted serving design. 0b shipped as Plan 140 Stage 2 on 2026-08-20, which also leaves Stage 4 a `docker-socket-proxy` to extend rather than a socket path to open. Stage 2 (PR #223, merge `50bba68`) adds two scraper-owned outcome counters, the `scraper` Prometheus job that never existed, `ct-solver-not-solving` + `ct-detail-fetch-failing` in `bool`-product form, and fixes D1's remaining partial-hour defect in the Plan 143 snapshot SQL. **24h soak to 2026-08-21 ~20:42 UTC; the D1 snapshot check lands at the 21:00 build.** Stages 3-4 not started |
| [137](plan_137_legacy_bronze_parquet_disposition.md) | Legacy bronze Parquet recovery and disposition | Draft — read-only inventory complete; no deletion authorized |
| [138](plan_138_public_surface_refresh.md) | README and public portfolio surface refresh | Draft — audit complete; analytics acquisition/database removal transferred to Plan 143, while public presentation remains here |
| [139](plan_139_test_suite_maintenance.md) | Test suite construction and maintenance | **Stages A+B complete 2026-08-18** (PR #213) — coverage reported at 88%, CI path 333s → ~260s; Stages C/D remain queued as opportunistic filler; analytics producer coverage transferred through Plan 136 to Plan 143 |
| [140](plan_140_service_health_contract.md) | Service health contract | **Stages 1, 2 and 3 deployed and verified in production 2026-08-20** (PRs #216, #221, #222) — `cartracker_container_health` publishes three states for all 28 running services from a dedicated scrape-time exporter behind `docker-socket-proxy`, `ct-service-down` covers all 9 scrape jobs under an exact-set-equality test, and a Service Health row opens the Infrastructure dashboard. The deploy found two defects worth more than itself: a single-file bind mount pins an inode, so `git pull` + SIGHUP reloaded the *old* Prometheus config while logging success (routed to Plan 144); and the first alert expression false-paged in six minutes on a recovered container, fixed with `== bool`. **24h soak is the only open gate**; Stage 4 is gated behind it |
| [141](plan_141_structured_log_ingestion_contract.md) | Structured log ingestion and dashboard contract | Draft — routed from Plan 135 closeout; parsing, labels, privacy policy, dashboards, and capacity soak not started |
| [142](plan_142_planned_host_maintenance.md) | Planned host maintenance and production quiescence | Draft — separate maintenance intent, truthful drain, checked-in apt/reboot procedure, and Plan 140-gated resume not started |
| [143](plan_143_analytics_serving_snapshot.md) | Analytics serving snapshot and reader consolidation | **Complete 2026-08-20** — PR #217 (`e5d3a46`) deployed the serving boundary; PR #218 (`a3cdd59`) corrected the Grafana ownership/cadence defects the first soak exposed. Corrected soak clean: 24 hourly publications, zero failed publishes, 3,580.9s worst-case freshness against a 4,500s threshold, zero lock conflicts, `/info` at 0.157s |
| [144](plan_144_deploy_script_hardening.md) | Deploy script hardening | **Complete 2026-08-21** — PRs #224 and #225 deployed and verified at `dd9e207`; `--restart prometheus` saw a real `starting` → `healthy` transition in 6s and verified inode 519823 on both sides. `--no-deps`, a health gate whose timeout is derived from the slowest healthcheck in `docker-compose.yml` and checked in CI, an intent-release rule that splits build failure (release) from partial recreation (hold), a `--restart` path that restarts single-file bind mounts and verifies the loaded inode, and a warning for peers that cached a recreated service's address. The deny-list moved to `healthcheck-exemptions.txt` so the poller and `TestServiceHealthCoverage` read one list. Grew from three defects to five: Plan 140 Stage 2 added the inode trap, Plan 136 D6 added the cached-address trap |
| [69](plan_69_terraform.md) | Terraform IaC | Draft — **moved from backlog to build-order row 13 on 2026-08-20** as a stated prerequisite of Plan 121. First slice is `terraform import` until `plan` shows no diff against production |

---

## Backlog

| Priority | Effort | Plan | Title | Resume trigger / blocker |
|---:|---|---|---|---|
| 55 | M | [66](plan_66_sql_injection.md) | SQL injection audit | Pull forward after any new public mutation surface or auth-boundary change |
| 52 | S | [122](plan_122_runtime_scraper_fetch_config.md) | Runtime scraper fetch configuration | Plan 136 telemetry shows timeout or solver tuning needs runtime control |
| 40 | L | [79](plan_79_multi_instance.md) | Multi-instance detail scraping | IP flagging or single-host throughput becomes the measured constraint. **This is also the plan that would create genuine multi-host need**, and therefore the honest trigger for Plan 88 |
| 38 | M | [94](plan_94_api_docs.md) | API documentation hub | Public or partner API consumption makes consolidated docs useful |
| 30 | M | [108](plan_108_deploy_trigger_endpoint.md) | Deploy trigger endpoint | Re-scope after Plan 136's narrower restart-authority design is proven |
| 25 | XL | **88** | Kubernetes | Multi-host scheduling/availability needs exceed Compose, not merely service count. See the control-plane note below before re-scoring this |

---

## Sequencing Rationale

**Plans 135/140/143/136/142/141 operational sequence** - Plan 135 closed the
storage blind spot. Plan 140 established the uniform container-health floor and
**deployed on 2026-08-20**, leaving only its soak and its gated Stage 4.
Plan 143 then made analytics freshness truthful at one serving boundary and
removed recurring metrics/public-page reads from DuckDB. Plan 136 resumes with
solver-outcome signals before any automatic restart is trusted. Plan 142 reuses
those health and drain primitives for deliberate
whole-host maintenance without granting application code reboot authority. Plan
141 then makes log parsing and dashboard semantics a tested contract. Plan 134
starts its warning-only observation window after 141 because
enforcement depends on a week of trustworthy evidence. Plan 133 was that small,
explicit prerequisite for Plan 132's packed artifact reparse and closed on
2026-08-20; Plan 132 should prove the recovery path before Plan 137 reuses
the same provenance and backfill safety at much larger scale. Plan 132 Stage 2
also carries a measurement Plan 133 could not make: the verifier drops caches by
design, so the `PACK_INDEX_CACHE_PACKS` 4-to-48 change is proven safe but not
yet proven effective, and a month-sized sequential reparse is where that shows. Most Plan 138 work
is independent, but its stats presentation consumes Plan 143; it belongs before
the next major platform milestone so the public surface and its source-controlled
work feed start from an accurate baseline.

**Plans 126-127 after the lakehouse/adaptive-refresh substrate** - The old
Plan 87 Kafka placeholder is superseded by Plan 126. The natural streaming seam
is the existing staging-event/outbox pattern, not direct app-to-broker writes.
Plan 126 should first prove Kafka-compatible event transport, replay, and a
low-risk consumer while preserving Airflow/batch parity. Plan 127 can then use
those events for adaptive scrape-control feedback once Plan 125 provides the
stable analytics substrate and Plans 112/113 clarify refresh-policy promotion.

**Plan 79 whenever needed** - IP flagging is not currently active.
Prerequisites all exist. Provision Oracle Cloud VMs and fan out the DAG when
needed. Note that Plan 79 is also the plan that creates real multi-host need,
which makes it the honest precondition for Plan 88 below.

**The accumulating control-plane cost, and why Plan 88 still scores 25**
(recorded 2026-08-20, after the question was raised directly). Several queued
plans are, in effect, hand-built orchestration:

| Plan | What it builds | Kubernetes equivalent |
|---|---|---|
| [142](plan_142_planned_host_maintenance.md) | Maintenance intent, drain, quiescence | `kubectl drain` / cordon |
| [136](plan_136_solver_recycle_and_liveness.md) Stage 4 | Restart authority for an unhealthy service | Liveness probe + `restartPolicy` |
| [140](plan_140_service_health_contract.md) Stage 2 | Container health as a metric — **delivered 2026-08-20** | `kube-state-metrics` |
| [108](plan_108_deploy_trigger_endpoint.md) | Deploy trigger endpoint | Updating a Deployment's image tag |
| [144](plan_144_deploy_script_hardening.md) | Readiness-gated deploys | Rolling update with readiness probes |

Priced together that is genuinely comparable to a single-node k3s migration, so
**the honest reading is that 25 understates it** — and nobody was summing those
rows. One of them is now paid: Plan 140 Stage 2 shipped on 2026-08-20 at roughly
250 lines of exporter plus a socket proxy, which is a data point for the
argument rather than against it — the `kube-state-metrics` equivalent was small,
and the part that was hard (three states, project scoping, a false-paging
expression) is domain judgment an orchestrator would not have supplied. Four
arguments still keep the score there:

1. **It would not have caught either 2026-08 incident.** The solver was a
   *healthy* container reporting `status:ok` at a 0% solve rate for eight hours;
   a Kubernetes liveness probe tells exactly the same lie, because it is the same
   probe. The apiserver failure it would have restarted — but Plan 136 Stage 4
   buys that specific win at S/M rather than XL.
2. **Every real win in this system has come from domain observability, not
   infrastructure** — solve-rate counters, extraction yield, pack-verification
   refusal, snapshot freshness. An orchestrator supplies none of them, and Plan
   136 Stage 2 remains the highest-value unbuilt work on this list.
3. **One node gives reconciliation without scheduling.** The service count is not
   the argument it appears to be: of 31 services there are 10 `build:` entries,
   three of which are one-shot/profile-gated and one of which shares another's
   image. Roughly six long-running first-party services sit behind ~20 vendor
   containers. Kubernetes does not reduce that count; it re-expresses it as
   Deployments, Services, PVCs, and ConfigMaps — typically 3-5x the YAML, plus a
   control plane on a box already running everything in 23 GB.
4. **It stalls the data-integrity chain.** Plans 132 and 137 are about not losing
   data, and an XL migration puts them behind it.

**The decision rule, so this stops being re-argued from feeling:** revisit Plan
88 when a second host actually exists — which Plan 79 is what creates. A
single-node migration is also the weakest version of the skills case for it,
since it teaches manifest syntax rather than the multi-node scheduling, ingress,
RBAC, and resource-budgeting work the technology is actually for.

**Plan 69 is the counter-example and moved accordingly.** Terraform was pulled
out of this backlog on 2026-08-20 into build-order row 13, because its trigger
("a second environment is approved") fires the moment Plan 121 starts, and
because it is the case where a marketable skill and a real engineering need
point the same direction. Plan 88 is not yet that case; Plan 69 is.

**Plans 114/129/130/131 bronze storage sequence** - Plan 114 measured sectioned
dedup and rejected it (-223%; MinIO's ~8 KB/object floor). Plan 129 shipped the
trained dictionary it recommended instead, cutting bytes ~73% logical / ~60%
physical while leaving object count untouched. Plan 131 is the remaining lever
and the only one that addresses **inodes**, which compression cannot; its Stage 0
must first re-read `df -i` and price a results-page retention policy, which may
be the cheaper answer. Plan 130 is the largest measured win and the only
irreversible one, so it stays blocked until the reversible options are exhausted
and the taxonomy gap is closed. **Plan 131 Stage 5 pushes that exhaustion out
~3 years**: run continuously it converts the inode deadline into a steady state
and leaves bytes as the binding constraint, at roughly 36 months of full
retention on the current disk. Retention/expiry for raw HTML is still unwritten
and is the gap behind all four.

**Plans 110-125 lakehouse/adaptive-refresh sequence** - Plans 110 and 111 are
the completed foundation: storage normalization and adaptive-refresh feature
outputs. Plan 117 resets the forward roadmap toward a portable local lakehouse.
Plan 120 provides production-derived fixture snapshots consumed by CI and local
development. Plan 112 proved Iceberg/MLflow reproducibility, but it is paused
before backtest/model gates because DuckDB still owns the analytics contract.
Plan 143 first establishes the versioned serving snapshot that Plan 125 Gate D
can keep stable while moving dbt/analytics from DuckDB toward Iceberg-native
tables. Once that substrate is stable, Plan 112 resumes policy backtesting and Plan 113
deploys only an approved, pinned policy config into ops claim logic. Plans 114,
121, and 119 can follow in whichever order is most useful: raw HTML retention
research, staging environment, and governance/catalog expansion.

**Plan 138 is mostly independent public-surface work.** It should land before
another major platform milestone adds more documentation drift. Its stats UI
depends on Plan 143, but it must not recreate that producer. Its copy must
describe Plan 125 as a proven migration track, not as the current production
serving path, and it does not block or change lakehouse, storage, or liveness
behavior.

---

## Superseded

| Plan | Title | Reason |
|------|-------|--------|
| [89](plan_89_ops_analytics_split.md) | Operational/analytics dbt split | Philosophy preserved; implementation superseded by Plans 93, 97, 96 |
| [90](plan_90_dbt_cleanup.md) | dbt decommission / dbt-duckdb migration | Superseded by Plan 102; DuckDB source layer done in Plan 96; the new forward migration is Plan 125 |
| [118](plan_118_dbt_spark_migration.md) | dbt migration from DuckDB to Spark-compatible execution | Superseded/refined by Plan 125, which uses the Iceberg proof from Plan 112 and makes DuckDB-to-Iceberg migration the explicit objective |
| **87** | Kafka event-driven layer | Superseded/refined by Plan 126, which keeps the Kafka-compatible streaming idea but roots it in the staging-event/outbox pattern |

---

## Completed

See [completed_plans.md](completed_plans.md) for the full archive. Keep this
small table newest-first: Plan 138's public project-updates snapshot consumes it
alongside the default build-order table.

| Plan | Completed | Public summary |
|---|---|---|
| [143](plan_143_analytics_serving_snapshot.md) | 2026-08-20 | Moved analytics metrics and the public statistics page onto a versioned snapshot published by the service that builds the data, removing recurring database reads from the public request path. |
| [133](plan_133_pack_read_path_hardening.md) | 2026-08-20 | Closed two gaps in how archived HTML is read back after compaction, so recovery jobs retry stranded work instead of abandoning it. |
| [114](plan_114_sectioned_html_artifact_audit.md) | 2026-08-10 | Measured sectioned HTML storage on production data, rejected it on storage economics, and identified dictionary compression and packing as the better reversible path. |
| [128](plan_128_false_block_detection.md) | 2026-07-20 | Fixed challenge-page misclassification, preserved cooldown state, reconciled cleanup behavior, and closed the historical-repair decision. |
| [111](plan_111_adaptive_detail_refresh.md) | 2026-07-06 | Built listing-state fingerprints, state runs, volatility features, and initial adaptive-refresh priority outputs. |
| [110](implementation_plan_110_storage_layout_hygiene.md) | 2026-07-06 | Normalized the storage foundation for the lakehouse and adaptive-refresh work. |
| [115](plan_115_detail_unenriched_circuit_breaker.md) | 2026-07-01 | Stopped successfully scraped but unenriched listings from entering a pathological 15-minute retry loop. |
| [95](plan_95_portfolio_landing_page.md) | 2026-05-04 | Shipped the first public portfolio landing page with pipeline context and live statistics. |
