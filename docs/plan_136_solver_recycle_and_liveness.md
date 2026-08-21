# Plan 136: Solver Recycle and Real Liveness — The Alert Fired During the Recovery

## Status

**Stage 0 complete and verified in production 2026-08-18. Stage 2 deployed to
production 2026-08-20 (PR #223, merge `50bba68`); its 24-hour soak closed on
2026-08-21 — the alert half green, the shape half inconclusive by construction.
Stages 3 and 4 not started.**

The soak proved both new rules quiet and the counters healthy, but it **did not
answer open question 2**: a healthy window contains no solver decay to read, so
Stage 3's recycle interval still has nothing to be chosen from. See
[the soak record](#24-hour-soak-record--2026-08-21).

Stage 0 shipped as PR #214, merge `8b2254b`; see
[Stage 0 production verification](#stage-0-production-verification-2026-08-18).
0a and 0c are deployed and proven; 0b was reassigned to
[Plan 140](plan_140_service_health_contract.md) Stage 2 rather than built here.
The unshipped Stage 1 prototype at commit `584f100` exposed the right failure
contract but the wrong serving boundary; [Plan 143](plan_143_analytics_serving_snapshot.md)
now owns its redesign, deployment, and soak.

[Stage 2](#stage-2--a-liveness-signal-that-does-not-pass-through-dbt) is
deployed; see its
[production deployment record](#production-deployment--2026-08-20). It ships:
two scraper-owned outcome counters, a `scraper` Prometheus job,
`ct-solver-not-solving` and `ct-detail-fetch-failing`, and D1's remaining
partial-hour defect fixed at its source. Two things changed from the plan as
written — the specified alert expression was the filtering-comparison shape that
false-paged Plan 140, and one rule cannot cover both solver failure shapes
because they move the solver counter in opposite directions. Both are written up
in that section.

**Stage 3 was deliberately not built alongside it.** Its recycle interval is
chosen from what these counters show (open question 2), and it needs
`POST /containers/*/restart` on the socket proxy Plan 140 left at `POST: 0` —
an authority expansion that deserves its own change.

Written 2026-08-15 after an 8-hour detail-scraping outage that no
alert caught. The only production action
taken during the incident was `docker restart cartracker-trawl`, which resolved
it.

**Extended 2026-08-18 after a second incident** — the Airflow apiserver wedged
on an exhausted connection pool and, again, **no alert caught the failing
component**; a downstream DAG failure raised it seven minutes later. Same shape,
different service, which is why it lives here rather than in its own plan. It
adds D4 and D5 and a new [Stage 0](#stage-0--the-apiserver-fixes-and-container-health-as-a-signal).
Still nothing applied; the only action taken was
`docker restart cartracker-airflow-apiserver`.

**Extended again 2026-08-20** with [D6](#d6--a-recreated-exporter-silently-orphans-every-long-lived-statsd-sender):
the Airflow scheduler had been sending its metrics to a dead UDP address for two
days, so seven of eight DAG panels were empty and `ct-pipeline-failures` was
blind. Found by a human looking at a dashboard. Again nothing alerted, and this
time **the thing that died was the detection mechanism itself.**

Three findings now share one root cause: **the health of a component is not a
signal this system collects.** Every one was found by noticing damage
downstream — never by an alert.

## The incident

| | |
|---|---|
| Started | 2026-08-14 21:00 UTC |
| Detected | 2026-08-15 ~04:45 UTC, **by a human noticing missing data** |
| Resolved | 2026-08-15 05:11:46 UTC, `docker restart cartracker-trawl` |
| Duration | ~8h 12m |
| Cost | ~250 listings pushed into 12–96h exponential cooldown; one hour of detail coverage lost per hour of outage |

`trawl` — the Cloudflare/Turnstile solver, third-party image
`ghcr.io/germondai/trawl:latest` — had been up 22 days. Its Turnstile solve rate
went from working to **0%** at 21:00. It still obtained `cf_clearance` cookies,
but pages stayed on the challenge interstitial; all 7 click attempts failed,
`/v1` returned HTTP 500, and the scraper fell back to plain `curl_cffi` and took
a 403 every time.

A plain restart fixed it completely: **2,541 of 2,545 detail artifacts in the
following 41 minutes were real pages** (≥20 KB vs the 6,623-byte challenge
page), adaptive delay recovered 30s → 0s, and the search-results path recovered
alongside it. No image pull was needed, so this was stale in-container state,
not an upstream Cloudflare change and not an IP ban.

### Two false leads worth recording

**The container was named the wrong thing.** `FLARESOLVERR_URL=http://trawl:8191`
— the env var kept its old name after the solver was swapped. A separate
`cartracker-flaresolverr` container is still running and has served **zero
requests since 2026-07-07**. It is pure misdirection during an incident.

**A bare `curl` from the VM returns Cloudflare Error 1020, "Sorry, you have been
blocked."** That looks exactly like an IP ban and is not one — it is curl's TLS
fingerprint tripping a stricter rule. With `curl_cffi impersonate='chrome'` the
same host gets `cf-mitigated: challenge`, identical to a control request from a
laptop on a residential IP. **Always re-test with a browser fingerprint before
concluding the egress IP is banned.**

---

## The second incident — the Airflow apiserver, 2026-08-18

Different component, same shape: the thing that broke was invisible, and
detection came from downstream damage.

| | |
|---|---|
| Started | 2026-08-18 ~01:31 UTC |
| Detected | 01:38, **by a `ct-pipeline-failures` page for a DAG that died downstream** |
| Resolved | 01:45:55 UTC, `docker restart cartracker-airflow-apiserver` |
| Duration | ~15 min |
| Cost | 7 scheduled DAG runs; one `scrape_listings` cycle (30 min of listing staleness). No data lost |

The apiserver exhausted its **SQLAlchemy connection pool** and stopped answering:

```
sqlalchemy.exc.TimeoutError: QueuePool limit of size 5 overflow 10 reached,
connection timed out, timeout 30.00
```

Every authenticated request runs `resolve_user_from_token` →
`RevokedToken.is_revoked(jti)` → a DB session, so once the pool starved,
**nothing could authenticate, including the healthcheck.** The container
accepted TCP and returned zero bytes for ten seconds, ten probes in a row.

The pool is Airflow's stock `5 + 10` and had never been tuned, while
`AIRFLOW__CORE__EXECUTION_API_SERVER_URL` routes every task's state through that
apiserver — so task concurrency consumes apiserver pool connections directly.
It held for four weeks and wedged **the day after two DAGs were added**
(`pack_bronze_html` and the Plan 135 Stage 4 watchlist). That timing points at
load growth rather than a leak, but the two are not yet distinguishable — see
Open questions.

Everything recovered on its own within five minutes of the restart, with no
manual triggering: the `*/5` DAGs succeeded at 01:50.

**What kept working is the interesting part.** Plan 131's July prune ran
straight through the outage *and* the restart, because its DAG holds no logic
and the work lives behind an HTTP endpoint in another container. See
[Plan 131](plan_131_packed_cold_storage.md#the-apiserver-outage-and-what-it-proved-about-the-shape--2026-08-18).

### Three false leads, all mine

Recorded because each was plausible, each was wrong, and the data killed them
faster than reasoning would have.

**"The disk is saturated."** Two heavy I/O jobs genuinely were running — the
July prune and Plan 135's `du` watchlist — and the plan doc had explicitly
warned that the latter competes with live work. It fit perfectly and was wrong:
`/proc/pressure/io` read `full avg10=8.91` with load 2.50. A quiet machine.

**"Postgres is out of connections."** Right mechanism, wrong side.
`pg_stat_activity` showed **27 of 100**. The limit was client-side in the
apiserver's own pool, and checking the server first cost a round trip.

**"The diagnostic tools are hanging, so the box must be sick."** They were not.
`docker exec ... psql` without `-it` sat at a password prompt, which looks
identical to a hang. `docker ps` and `docker inspect` had been returning
instantly the whole time — which was already proof the daemon was fine.

> **The reading that actually solved it: zero bytes with 0.00% CPU and 1.40%
> memory.** A process consuming nothing while answering nothing is blocked on a
> lock or a pool, never on resources. Resource exhaustion burns something.

### D4 — Container health is not a signal anywhere

The apiserver was `(unhealthy)` in `docker ps` and **nothing anywhere reacted**:

- **`restart: unless-stopped` ignores healthcheck state.** It acts on container
  *exit*. An unhealthy-but-running container sits there forever, and this one
  would have.
- **`ct-service-down` matches `up{job=~"ops|processing"}`** — Airflow is not in
  that regex.
- **Adding it would not have helped.** The `airflow` Prometheus job scrapes
  **statsd-exporter**, not the apiserver. statsd-exporter was perfectly up and
  reporting throughout; you can see it in the alert that did fire
  (`instance = statsd-exporter:9102`). A healthy metrics sidecar in front of a
  dead service is exactly the stale-proxy problem D2 describes, one layer out.

So the only signal was `docker ps`, which nothing watches. Detection came seven
minutes late, from a DAG failing downstream — the same failure mode as the
solver incident, which is why it belongs in this plan.

### D5 — `ct-pipeline-failures` emits a label-less duplicate

Cosmetic but confusing mid-incident. The rule produced **two** instances, one
correctly labelled `dag_id = scrape_listings` and one rendering
`summary = DAG [no value] failed` — a series without a `dag_id` label matching
the same expression. During triage it reads as a second, unidentified failure.

---

## The first incident's three defects

D1-D3 below predate D4 and D5 and belong to the solver outage. The numbering is
chronological by discovery, so it runs out of document order — D4 and D5 are
written above with the incident that produced them.

### D1 — The volume alert is inverted

`ct-scrape-volume-drop` alerts when `cartracker_observation_count_last_hour < 100`
for 30m. Measured across the incident:

| Time (UTC) | Gauge | Reality |
|---|---|---|
| 08-14 21:30 → 08-15 02:00 | **12,204** (frozen 4.5h) | scraping dead |
| 08-15 02:30 → 05:11 | 44 | scraping dead |
| 08-15 05:11 → now | **still 44** | scraping healthy, 2,545 artifacts |

The alert **stayed silent for the entire outage and began firing at 05:51 —
forty minutes into the healthy recovery.** It is not merely insensitive; it is
anti-correlated with the thing it monitors.

Root cause was in the pre-Stage 1 implementation of
`ops/metrics/analytics_gauges.py` (then named `duckdb_gauges.py`):

```sql
SELECT observation_count, artifact_count
FROM main.mart_scrape_volume ORDER BY hour DESC LIMIT 1
```

The gauge is the newest row of a **dbt mart**. It therefore sits downstream of
scrape → processing → silver flush → dbt build → DuckDB. It lags that whole
chain, it reports a partially-filled current hour as a real drop, and when the
pipeline stalls it reports the last good hour forever. This is a data-quality
metric being used as a liveness alarm, and it cannot do that job.

### D2 — Gauges silently retain stale values

The same pre-Stage 1 module ended with:

```python
except Exception as e:
    if "Conflicting lock" in str(e):
        logger.warning(f"DuckDB connection skipped (write lock held by dbt): {e}")
```

When dbt holds the write lock the refresh is skipped and **every gauge keeps its
previous value**. `prometheus_client` has no staleness concept, so Prometheus
scrapes a stale number that is indistinguishable from a live one, and every
alert downstream evaluates it as current. Observed 35 lock-skips in 24h.

This defect is not specific to the scrape-volume gauge — it silently degrades
`cartracker_block_events_last_hour`, `cartracker_stale_listings_pct`,
`cartracker_extraction_yield_last_day`, and both cooldown gauges the same way.

### D3 — Every threshold assumes a fast failure

This outage failed *slowly*. Each request spent ~90s in a doomed solve before
giving up, which throttled the error rate below every tripwire:

| Alert | Threshold | Actual during outage | Fired |
|---|---|---|---|
| `ct-403-log-spike` | >10 403s per 5 min | ~2–3 per 5 min | no |
| `ct-block-events-spike` | >200 block events/hr | 28–38/hr | no |
| `ct-cooldown-backlog` | >5,000 listings | 415 total | no |
| `ct-scrape-volume-drop` | <100 obs/hr for 30m | stale gauge (D1) | **inverted** |

A solver that fails fast would have tripped the 403 alert in minutes. A solver
that fails slowly is invisible. Thresholds tuned to burst failures do not detect
throughput collapse.

---

## D6 — A recreated exporter silently orphans every long-lived statsd sender

**Found 2026-08-20 by a human noticing empty dashboard panels. Confirmed and
fixed the same day; the detection gap is not fixed.**

Third instance of this plan's thesis, and the first one where *the detection
mechanism itself* was what died.

| | |
|---|---|
| Started | 2026-08-18 17:03:06 UTC |
| Detected | 2026-08-20 ~21:05, **by a human looking at the Pipeline Health dashboard** |
| Duration | **~2 days 4 hours** |
| Resolved | 21:11:53 UTC, `docker restart cartracker-airflow-scheduler` |
| Cost | No data lost. Two days with no DAG-level monitoring and a **blind failure alert** |

### What broke

Seven of the eight Airflow panels on Pipeline Health returned nothing, and so
did **`ct-pipeline-failures`** — the alert that detected the apiserver incident
in this very plan. Its `noDataState` is `OK`, so a dead input rendered as a quiet
green rule rather than as a problem.

Only `airflow_ti_successes` survived, and that survivor is the whole diagnosis.

### The mechanism

`statsd-exporter` was recreated at 17:03:06 on 2026-08-18 — **the Plan 140 Stage
1 deploy**, which gave it a `healthcheck:` block and therefore a new container
and a new IP. The last sample of every scheduler-emitted metric is 17:07.

The Python statsd client resolves its destination **once, when it is
constructed**, and then `sendto()`s the cached address. The scheduler had been
running since 04:58 that morning and never restarted, so from 17:03 it addressed
UDP packets to an IP nothing was listening on. **UDP fails silently** — no
exception, no log line, no error metric.

That is exactly why `airflow_ti_successes` lived: task metrics come from
short-lived LocalExecutor task processes, which construct a fresh client and
resolve DNS every run. Long-lived process, dead metrics; short-lived processes,
working metrics. Split by process lifetime, not by metric.

Evidence, in the order it settled the question:

| Observation | Rules out |
|---|---|
| Last sample 17:07 vs exporter recreated 17:03:06 | Coincidence |
| Exporter healthy, `statsd_exporter_udp_packets_total` = 67,099 | A dead or deaf exporter |
| Zero live `airflow_scheduler_*`/`dagrun_*`/`pool_*`; task metrics live | Airflow not emitting at all |
| Scheduler `restarts=0`, running since before the recreate | A scheduler crash |
| `gethostbyname` in the scheduler container returns the *correct* new IP | Broken DNS |
| Its UDP sockets are unconnected (`rem_address 0.0.0.0:0`) | A connected socket that would have errored |
| **Restart alone restored them in 80s, no config change** | Everything else |

### Why this belongs here and not in Plan 140

Plan 140 Stage 1 *caused* it, but Plan 140 could not have caught it and neither
could anything else in the stack:

- `cartracker_container_health` reports statsd-exporter **healthy**, and it is.
  The container is fine; the pipe into it is dead. This is D4's "healthy metrics
  sidecar in front of a dead thing" with the layers inverted.
- `up{job="airflow"}` is **1** throughout, because that job scrapes the
  exporter, not the scheduler.
- D2 said a gauge can retain a stale value and look live. This is the sharper
  version: the series **vanish**, and `noDataState: OK` turns absence into
  silence. Absence is not the safe direction for a metric that should always be
  present.

**The generalisation is the important part.** Any long-lived process holding a
cached peer address loses it when the peer is recreated, and a deploy that
recreates containers is the normal case, not an exotic one. Anything that talks
UDP or holds a long-lived resolved address is exposed. The failure is silent by
construction.

### What is fixed and what is not

Fixed by the restart, confirmed: `airflow_pool_open_slots` and
`airflow_scheduler_heartbeat` returned within 80 seconds, and
`airflow_dagrun_duration_success{dag_id="..."}` at the next DAG completion
(21:15). Panels 4, 5 and 8 and `ct-pipeline-failures`'s input are live again.

Not fixed, and owed:

1. **`ct-pipeline-failures` must treat NoData as a failure.** For a metric that
   should always be present, `noDataState: OK` is the defect. Plan 143 already
   set this precedent with `ct-metrics-freshness`.
2. **A staleness signal for the Airflow scrape**, since `up` cannot see this.
3. **A deploy-time check for the class**, not this instance: recreating a
   service can orphan long-lived senders. `promtail` and `postgres-exporter` are
   worth auditing for the same exposure.

Items 1-3 are the reason this is a defect and not just an incident log: the
restart fixes today, and nothing yet would catch the next one.

### Two older defects this investigation uncovered, which the restart did not fix

Chasing D6 answered "why is the dashboard empty?" only partly. Three of the
eight Airflow panels were broken *before* 2026-08-18 and by unrelated causes.
Recording them here so they are not misfiled as D6 fallout:

| Panel | Queries | Reality | Cause |
|---|---|---|---|
| 3. Scheduler Tasks Running | `airflow_scheduler_tasks_running` | only `..._executable` and `..._starving` exist; **no data in 30 days** | Airflow 3 metric rename |
| 6. DAG Scheduling Delay | `airflow_dagrun_schedule_delay` | Airflow emits `airflow.dagrun.first_task_scheduling_delay.<dag_id>` | Airflow 3 metric rename — **and `grafana/statsd_mapping.yml` still maps the old `airflow.dagrun.schedule_delay.*`**, so it arrives unmapped as `airflow_dagrun_<dag_id>_first_task_scheduling_delay` with the dag_id welded into the name |
| 2, 7. Task Failures / Success Rate | `airflow_ti_failures` | absent until a task actually fails | Structural: a counter that has never fired has no series |

Panel 6 therefore needs **two** edits, a mapping entry and a panel expression,
and fixing only one leaves it blank. Panels 2 and 7 should render `0` rather
than "No data" when nothing has failed — the same defect Plan 140 fixed for its
health tiles, and pinned there by
`test_health_tiles_read_zero_rather_than_no_data`. A failures panel that reads
"No data" when healthy is indistinguishable from one whose metric has died,
which is precisely how D6 hid for two days.

**The general lesson is that nothing tested these panels against the metrics
that actually exist.** Two are querying names Airflow stopped emitting at the
3.x migration and nobody noticed, because an empty panel and a healthy system
look identical. That is a dashboard-contract problem and belongs with
[Plan 141](plan_141_structured_log_ingestion_contract.md), which owns dashboard
selectors — with the caveat that these are Prometheus selectors, not Loki ones.

## Goal

1. Detect a solver outage in **under 15 minutes**, from a signal that does not
   pass through dbt.
2. Never let a gauge report a stale value as if it were live. Plan 143 owns the
   analytics-serving implementation and freshness alert that satisfy this goal.
3. Recycle `trawl` on a schedule so 22-day state rot cannot accumulate.
4. Restart `trawl` automatically when it fails, without a human in the loop, and
   without pushing in-flight listings into cooldown.
5. **Never let a container sit unhealthy unnoticed.** A failing healthcheck must
   reach an alert, on any service, without depending on a metrics sidecar that
   stays up while the thing behind it is dead.

---

## Stage 0 — The apiserver fixes, and container health as a signal

Smaller than everything below and independent of it. Addresses D4 and D5, plus
the sizing that caused the second incident.

**0a. Size the apiserver pool.** Apiserver-**only**, not on the shared anchor:

```yaml
  airflow-apiserver:
    environment:
      <<: *airflow-common-env
      AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE: '20'
      AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW: '20'
```

**The connection budget is the constraint, and the naive fix violates it.**
`x-airflow-common-env` is a YAML anchor shared by four services (apiserver,
scheduler, dag-processor, triggerer), so setting the pool there multiplies by
four against Postgres's `max_connections = 100`. At `20 + 20` on all four that
is a 160-connection worst case — trading this outage for a Postgres-side one.
Apiserver-only gives 40 there plus 15 each for the other three: **85 against
100**, with measured steady state at 27.

This is a service *config* change, so it needs `docker compose up -d --no-deps
airflow-apiserver`. **`docker restart` silently reuses the old config** — the
same trap Plan 135 Stage 1 hit with node-exporter, where the container came back
looking healthy with the old flags.

A test should assert the pool settings exist **and** that the worst-case sum
across the Airflow services stays under `max_connections`. That budget is the
thing a future edit will silently break.

**0b. Make container health alertable.** The gap D4 describes is that
`(unhealthy)` exists only in `docker ps`. Proposal: emit
`cartracker_container_health{container=...}` (1 healthy / 0 unhealthy / absent
if not running) and alert on any 0 for 5m.

Plan 135 Stage 4 just built the delivery mechanism — a scheduled job writing
atomically to node-exporter's textfile collector — so this is a second producer
into proven plumbing rather than a new exporter. It needs read-only Docker
socket access, which Stage 4 below already proposes via `docker-socket-proxy`;
if that lands first, read health through it. **Do not add a second socket
path.**

The alternative is cAdvisor, which is heavier, and whose health semantics are
less direct than reading `.State.Health.Status`.

**0c. Fix `ct-pipeline-failures`' label-less duplicate** (D5). Constrain the
expression so a series without `dag_id` cannot match — e.g. requiring
`dag_id != ""` — so one failure renders as one instance.

**Verify:** `docker inspect --format '{{json .State.Health}}'` shows a clean
streak after 0a; the pool settings appear in `.Config.Env` (not merely in the
compose file); a deliberately stopped non-critical container produces a `0` and
an alert; and `ct-pipeline-failures` emits exactly one instance per failed DAG.

### Stage 0 production verification, 2026-08-18

Deployed as PR #214 (merge `8b2254b`). **0b is not part of this** — see
[Stage 0b's reassignment](#0b-was-reassigned-to-plan-140) below.

**The two fixes needed opposite deploy commands**, which is worth recording
because getting it backwards produces a healthy-looking container running the
old config — the trap Plan 135 Stage 1 hit with node-exporter:

| Change | Command | Why |
|---|---|---|
| 0a | `docker compose up -d --no-deps airflow-apiserver` | `environment:` is service *config*; `docker restart` reuses the existing container's config |
| 0c | `docker restart cartracker-grafana` | `./grafana/provisioning` is a bind mount, so the file was already inside the container. Compose sees no drift and `up -d` is a no-op; Grafana only re-reads provisioning at startup |

#### 0a

| Check | Result |
|---|---|
| Env in the **running** container | `SQL_ALCHEMY_POOL_SIZE=20`, `SQL_ALCHEMY_MAX_OVERFLOW=20` |
| Container was recreated, not restarted | `Created: 2026-08-18T15:39:38Z` — fresh |
| **Airflow actually parsed it** | `airflow config get-value database sql_alchemy_pool_size` → `20`; `..._max_overflow` → `20` |
| Anchor did not leak | `0` pool vars on scheduler, dag-processor, triggerer |
| Health | `Status: healthy`, `FailingStreak: 0` |
| `pg_stat_activity` | **14 of 100** — `airflow_user` 7, `cartracker` 2, `metrics_user` 1, 4 unnamed |

The `get-value` row is the one that matters. The env var being present and
Airflow honouring it are different claims, and only the second is the fix.

**This corrects the headroom warning written when 0a was authored.** That note
estimated non-Airflow consumers at ~17 (from a 27-total reading with Airflow at
~10) and concluded worst case was ~102 against `max_connections=100`. Measured:
non-Airflow is **7**, so worst case is **92**. The correction is real but should
not be over-read — the box was four minutes past a recreate and quiet, while the
27-total reading was taken under load. Treat 92 as the optimistic end.

#### 0c

Validated in **both directions**, because this change had a failure mode worse
than the defect it fixes: a guard matching nothing would silence the alert
permanently, and a silent alert beats a duplicated one on ugliness while losing
badly on consequence.

The defect was real. Three series exist and exactly one carries no `dag_id`:

```
airflow_dagrun_duration_failed_count{job="airflow"}                2   <- no dag_id
airflow_dagrun_duration_failed_count{dag_id="orphan_checker"}      1
airflow_dagrun_duration_failed_count{dag_id="results_processing"}  1
```

Counts: **3 unguarded, 2 guarded, 1 label-less.** The label-less series reads
`2`, exactly the sum of the other two — so it is an unmapped statsd aggregate
rather than an unknown DAG, which also explains why it tracked real failures
closely enough to look plausible during triage.

The guarded selector still matches 2, so the alert can still fire. Confirmed
end-to-end against Grafana's own rule store and instance set, not the file:

```
dag_id="orphan_checker"      -> "DAG orphan_checker failed"      Normal
dag_id="results_processing"  -> "DAG results_processing failed"  Normal
totals: {normal: 2}
```

Two named instances; the `DAG [no value] failed` instance is gone. Both carry
`activeAt: 2026-08-18T01:56:00Z` — these are the apiserver incident's own DAG
failures.

#### What this does not settle

**Open question 4 (undersized vs leaking) is untouched by this result**, because
a clean verification is consistent with both — raising the pool fixes sizing and
only delays a leak. New baseline: `airflow_user` at **7**, roughly two minutes
after the recreate, against the **10** recorded shortly after the 01:45 restart.
That needs sampling across several days, not a single reading.

### 0b was reassigned to Plan 140

Not deferred — **rebuilt elsewhere**. Checked against the compose file before
writing any of it: Docker reports no health status at all for a container
without a healthcheck, and only **7 of 31** services have one. The metric this
stage described would have been blank for the other 24, and a service with no
healthcheck would have been indistinguishable from a healthy one.

[Plan 140](plan_140_service_health_contract.md) Stage 2 builds the same metric
with a third state (`-1` for "no healthcheck configured") *after* its Stage 1
adds the missing healthchecks, so it covers everything the day it ships. The
socket-path decision this stage raised — `docker-socket-proxy` versus a direct
mount — travels with it, and Stage 4 below still owns the restart verb.

---

## Stage 1 — Analytics gauge freshness — TRANSFERRED TO PLAN 143

The behavioral requirements survive unchanged:

- failed or invalid analytics values become NaN rather than retaining a silent
  stale value;
- `cartracker_metrics_last_success_timestamp_seconds` advances only after a
  valid publication;
- `ct-metrics-freshness` alerts after one hourly publication cadence plus 15
  minutes of grace (4,500 seconds) and fails loudly on no data or evaluation
  errors; and
- the seven existing metric names remain stable for Grafana.

Pre-PR review showed that implementing those rules solely inside this incident
plan would preserve the wrong architecture. `ops` already had two independent
DuckDB reader paths, the public page used a transient Postgres queue as a
freshness source, SQL was being added directly to Python, and Plan 138 proposed
a second collector. [Plan 143](plan_143_analytics_serving_snapshot.md) now owns
the complete serving boundary: saved and integration-tested SQL, a durable
post-build snapshot, direct `dbt_runner` Prometheus export, removal of analytics
reads from `ops`, shared connection/query conventions, deployment, and soak.

Commit `584f100` is retained as prototype evidence and tests, not as a deployable
Stage 1. Plan 136 resumes at Stage 2 after Plan 143 establishes the freshness
contract.

## Stage 2 — A liveness signal that does not pass through dbt

**Implemented 2026-08-20. Not yet deployed.**

Add a **solver outcome counter owned by the scraper**, which already knows every
outcome at the moment it happens:

- `cartracker_solver_requests_total{outcome="ok|challenge|error"}`
- `cartracker_detail_fetch_total{outcome="ok|403|error"}`

Prometheus did not scrape the scraper at all; it is now a job in
[prometheus/prometheus.yml](../prometheus/prometheus.yml) alongside `ops` and
`processing`, and `scraper` was added to `ct-service-down`'s job set in the same
change — that rule's set is asserted equal to `prometheus.yml`'s, so the counters
being scraped and the scraper's own liveness being watched are one edit.

### The alert as specified would have false-paged

The expression this stage was written with is the exact shape Plan 140 shipped
and corrected five days earlier:

```promql
rate(cartracker_solver_requests_total{outcome="ok"}[15m]) == 0
  and rate(cartracker_solver_requests_total[15m]) > 0
```

`== 0` and `and` are both *filtering* operators. The moment the solver recovers,
the expression returns **no series at all**, and Grafana's `reduce: last` over
the 600s `relativeTimeRange` then keeps the last bad value alive for the rest of
that window — which is how `flaresolverr` stayed firing for eleven minutes after
recovering on 2026-08-20. Both rules below are written as a product of `bool`
comparisons instead, so exactly one series exists at every evaluation and it
reads 0 on the first evaluation after recovery.
`test_solver_alerts_cannot_drop_a_recovered_series` pins it.

### Two rules, because the solver fails in two shapes

This is the correction that changed the design, and it came from reading
`_CF_SESSION_TTL` rather than from the incident write-up. The two shapes have
**opposite** signatures on the solver counter:

| | Refusing (2026-08-14) | Lying |
|---|---|---|
| What happens | `/v1` returns 500; `get_cf_credentials` raises before assigning `_cf_credentials_expires_at` | `status: ok`, real `cf_clearance` cookies, interstitial behind them |
| Credentials cached? | **No** — every fetch re-bootstraps | Yes, the full 25 minutes |
| Solver request volume | Spikes to fetch rate | Unmoved at the healthy ~2.4/hour |
| Counter that sees it | `cartracker_solver_requests_total` | `cartracker_detail_fetch_total` |

So a single rule over the solver counter cannot cover both, and the plan's
original 15-minute window was sized against an assumption that does not hold:
a *healthy* system bootstraps roughly 2.4 times an hour, so
`rate(solver_total[15m]) > 0` is legitimately false most of the time. It happens
to work for the refusing shape only because that shape drives the rate up.

- **`ct-solver-not-solving`** — zero `ok` and more than five non-`ok` solver
  attempts in 15m, `for: 5m`. Fast: six failed bootstraps accumulate inside one
  batch, so this trips within minutes of the first failing run, comfortably
  under the plan's 15-minute goal. The failure side is `outcome!="ok"`, not
  `outcome="error"`, so `challenge` counts toward it.
- **`ct-detail-fetch-failing`** — zero `ok` among more than twenty detail
  fetches in 20m, `for: 5m`. Shape-independent, and the only one that sees a
  lying solver. The 20m window is chosen against the `*/15` schedule so it always
  spans a whole cycle: detail traffic is bursty (~100 fetches in about two
  minutes, then idle), and a window shorter than the schedule would keep falling
  into zero-traffic stretches where the volume guard reads 0 and resets the
  `for` — quieter, not faster. `test_the_detail_window_spans_a_whole_scrape_cycle`
  reads the schedule out of the DAG and enforces it.

Both guards are volume thresholds rather than `> 0`, because "nothing succeeded"
is trivially true of an idle scraper and one transient error in a quiet window
should not page.

### Why the counter classifies the page rather than trusting the solver

`challenge` is the outcome that makes the lying shape nameable instead of merely
absent, and it cannot come from the solver's own report: `trawl` returned
`status: ok` from its API and `status:ok` from its healthcheck for all eight
hours. `_solver_outcome` therefore reads the returned page's title. The marker
set moved to [shared/challenge.py](../shared/challenge.py) so `processing`'s
block classifier and this counter cannot drift; `processing` keeps its
`initial-activity-data` safety gate, which does not apply here because the
bootstrap URL is the homepage and carries no such blob.

Credentials are still cached on a `challenge`, exactly as before. Refusing to
cache an interstitial would re-bootstrap on every request and hammer the solver
hardest at the moment it is already failing — that is a behaviour decision for
Stage 4's circuit breaker, not for telemetry.

### D1's remaining half, fixed here

Plan 143 fixed the half of D1 where a DuckDB lock conflict left every gauge
silently retaining its last value. The other half was still live and is the
likely mechanism behind `ct-scrape-volume-drop` firing at 05:51, forty minutes
into a healthy recovery: `analytics_metrics_snapshot.sql` selected `MAX(hour)`
from a mart bucketed on `date_trunc('hour', fetched_at)`, and the build runs at
`0 * * * *`. That is the hour *currently in progress*, holding whatever few
minutes of data had flushed — published as an hourly total, against a threshold
of 100. The gauge's own description already read "the most recent **complete**
scrape hour"; the SQL now excludes the in-progress hour, and `data_through`
moves with it so the freshness field names the hour the counts describe.

The rule keeps its place as a **data-quality** signal and its annotation now says
so, pointing at the two rules above for liveness. It sits downstream of the whole
pipeline and cannot answer "is scraping working now" — reading it as though it
could is what cost eight hours.

> Verify: replay the incident by pointing the scraper at a deliberately broken
> solver URL in staging; alert fires within 15 min.

### Production deployment — 2026-08-20

Deployed to `50bba68` (PR #223) at 20:42 UTC. Deploy intent was declared first
with zero in-flight executions and released afterwards — unlike Plan 140 Stage
2's deploy, which correctly skipped it, because this one rebuilds `scraper`,
`processing` and `dbt_runner` and therefore touches the scrape and processing
paths.

| Check | Result |
|---|---|
| Prometheus reading the **new** config | `job_name: scraper` present in the running file |
| Scrape targets | **10** of 10 up, including the new `scraper` |
| Six outcome series | all present at `0`, `job="scraper"`, **before any traffic** |
| `ct-solver-not-solving` expression | 1 series, value 0 |
| `ct-detail-fetch-failing` expression | 1 series, value 0 |
| Rules provisioned | 20 (18 + 2), both new ones `health: ok`, inactive |
| `ct-container-unhealthy` | 28 of 28 `Normal` through three container recreates |

The last row is Plan 140's soak evidence arriving early and by accident. The
false page it corrected was *triggered by a container restart*, and this deploy
restarted three; all 28 instances stayed Normal. That is the `== bool` fix
tested against the exact scenario that broke the filtering form.

**The single-file bind-mount trap reproduced exactly**, hours after Plan 140
first documented it, and was caught only because that finding said to look:

```
host:      519823
container: 519700   <- still the pre-pull inode
```

`docker restart cartracker-prometheus` re-resolved it to `519823`. A SIGHUP
would have logged a successful reload of a config with no `scraper` job in it.
This is now twice in one day and belongs in the deploy procedure rather than in
an operator's memory — routed to
[Plan 144](plan_144_deploy_script_hardening.md).

#### D1's partial-hour fix, verified at the 21:00 build — green

The one part the deploy could not check on the spot, because it needed a
scheduled `hourly_analytics_refresh` to run.

| Check | Result |
|---|---|
| Snapshot published | `cartracker_analytics_snapshot_refresh_success` = **1**, in 0.130s |
| `last_success_at` | 21:01:23 UTC — the 21:00 build |
| `data_through` | **2026-08-20T20:00:00Z** — the last *complete* hour, not 21:00 |
| `cartracker_observation_count_last_hour` | **8,133** |

The old behaviour is visible in the gap between those last two rows. At 21:01
the unfixed query would have selected the 21:00 bucket — about one minute of
data — and published it as an hourly total against a threshold of 100. It now
publishes 8,133 for a genuinely complete hour, and `/info` names the hour the
count actually describes.

#### First live counter readings, 21:02 UTC

Twenty minutes after deploy, one `scrape_detail_pages` cycle in:

```
cartracker_solver_requests_total{outcome="ok"}         1
cartracker_solver_requests_total{outcome="challenge"}  0
cartracker_solver_requests_total{outcome="error"}      0
cartracker_detail_fetch_total{outcome="ok"}          457
cartracker_detail_fetch_total{outcome="403"}           0
cartracker_detail_fetch_total{outcome="error"}         1
```

**This is the measurement that retroactively justifies the two-rule split.** One
solver bootstrap against 457 detail fetches — a 457:1 ratio, and a healthy solver
rate of roughly 3/hour against the ~2.4/hour predicted from the 25-minute
`_CF_SESSION_TTL`. The plan's original single rule guarded on
`rate(solver_total[15m]) > 0`, which on this evidence is **false for most of a
healthy hour**. It would have been a blind alert for long stretches, and the
reason is a caching constant, not anything the incident write-up recorded.

It also sizes both guards against reality: healthy non-`ok` solver volume is 0,
well under `> bool 5`; and 457 fetches per cycle sits far above
`ct-detail-fetch-failing`'s `> bool 20`.

#### 24-hour soak record — 2026-08-21

Read at 16:35 UTC, **19h 50m** into the 24 hours from the 20:42 deploy. Called
early by decision. The alert half is settled and the remaining 4h 10m cannot
change it; the shape half is not settled, and as argued below a further four
hours would not have settled it either.

##### The alert half — green

| Gate | Evidence |
|---|---|
| `ct-solver-not-solving` inactive | **1,189 range evaluations, never non-zero.** Grafana logged zero state-change annotations for it |
| `ct-detail-fetch-failing` inactive | **1,189 range evaluations, never non-zero.** Zero annotations |
| Exactly one series each | Both held a single series at every evaluation — the property the filtering form lacks, now observed across a full day rather than at one deploy-time reading |
| Counters healthy throughout | Solver 48 `ok`, **0 `challenge`, 0 `error`**; detail 20,312 `ok`, 66 `403`, 4 `error` |
| Scrape targets | 10 of 10 up |

**The volume guard was exercised for real, not merely asserted.** In 14 of the
238 five-minute checkpoints the 20-minute `ok` count was genuinely **0** — the
overnight trough, 04:09–04:59 and 05:39–05:44 UTC. Every one of those windows
also had a *total* detail volume of 0, so `> bool 20` held the rule quiet.
**Zero windows cleared both conditions.** This is precisely the failure mode the
plan predicted when it rejected `> 0` in favour of a volume threshold: "nothing
succeeded" was trivially true of an idle scraper for the better part of an hour
last night, and a `> 0` guard would have paged on it.

The solver rate also came in where the caching constant said it would:

| Window | `ok` solves/hour |
|---|---|
| last 21h | 2.29 |
| last 10h | 2.40 |
| last 5h | 2.20 |
| last 1h | 3.01 |

Against ~2.4/hour predicted from the 25-minute `_CF_SESSION_TTL`. The 21:02
deploy-day reading of one bootstrap per 457 fetches was not a fluke of one
cycle.

##### The shape half — inconclusive, and not for want of waiting

Open question 2 asks whether the solve rate **decays gradually or falls off a
cliff**, because that choice sets Stage 3's recycle interval. The soak cannot
answer it:

- `trawl` has been up since 2026-08-18 04:28 UTC with **0 restarts** — about
  3.5 days.
- The 2026-08-14 outage followed **22 days** of uptime.
- Across the whole window the rate is flat (2.2–3.0/hour, no trend) and
  `challenge` and `error` are **0 at every hour**.

A 24-hour window taken 3.5 days into a cycle whose known failure horizon is
three weeks contains no decay signal to read. That is a property of the
question, not a defect in the counters — the counters are working, and their
flatness is the healthy baseline this plan wanted. **What it means is that
Stage 3 must not pick a recycle interval yet.** The honest next step is an
observation window measured in weeks, either until the rate visibly bends or
until `trawl` approaches the 22-day mark that preceded the incident. Picking an
interval now would be guessing with extra steps, which is the thing the plan
explicitly told itself not to do.

## Stage 3 — Scheduled recycle

A weekly `trawl` restart, which on this evidence would have prevented the
outage outright.

**The gotcha that makes this non-trivial:** a naive restart mid-batch fails
every in-flight request, and the scraper's 403 handler pushes each of those
listings into a 12-hour cooldown. A careless recycle inflicts a small version of
the very outage it prevents.

So the recycle must be **drain-aware**:

1. Pause claiming (the `scrape_detail_pages` DAG has `max_active_runs=1`, so
   waiting for the current run to finish is sufficient).
2. Confirm no active job via the scraper's existing
   `/scrape_results/jobs/completed`.
3. Restart `trawl`; wait for `/health` to report both browsers warm (~4s
   observed).
4. Resume.

Cadence: weekly. Uptime at failure was 22 days, so weekly carries a 3× margin.
This is a judgment call, not a measured optimum — Stage 2's counters will show
whether solve rate degrades gradually (tighten it) or falls off a cliff
(leave it).

> **The "22 days of uptime" framing needs revisiting before the interval is
> chosen.** [Plan 124](plan_124_trawl_memory_guardrails.md)'s 2026-08-18
> verification found `camoufox-bin` is OOM-killed inside the container roughly
> **every 1.5–4 days** — 12 times in a month, at a consistent ~3.2–3.5 GB. The
> browser processes are therefore *already* being recycled involuntarily, far
> more often than weekly. Whatever rots over 22 days cannot be browser process
> age; it has to be container-level state that survives a `camoufox-bin`
> restart. A weekly `docker restart` clears different state than these kills do,
> and the interval should be chosen against *that* state, not against uptime.
>
> Related: the 2026-08-14 outage contains one of those kills, at 04:28 on Aug 15
> — **7.5 hours after** the solve rate hit 0%, and at a lower rss than usual.
> Symptom, not cause.

## Stage 4 — Automatic restart

### Why the obvious approach does not work

The standard pattern is a Docker healthcheck plus an autoheal sidecar
(`willfarrell/autoheal`) that restarts containers marked `unhealthy`.
**It would not have fired.** `trawl`'s healthcheck is
`curl -sf http://localhost:8191/health`, which reports pool liveness only; it
returned `status:ok` throughout, and the container showed `(healthy)` across all
eight hours.

Making the healthcheck detect the real failure means making it perform a real
solve — 30–90s per probe, hammering cars.com on every interval, and liable to
false-positive on a single legitimately hard challenge. That trade is not worth
it.

### The signal we already have

The scraper knows. It logged `FlareSolverr/CF session failed (500 ...)` on every
single request for eight hours. Drive the restart from **outcome**, not from
health:

> After N consecutive `trawl` 500s (suggest N=5, ~10 min at observed pacing),
> trip a circuit breaker → request a recycle → reset the counter. Rate-limit to
> at most one automatic recycle per hour, and alert on every one.

Two guardrails matter. **The cooldown interaction from Stage 3 applies here
too** — while the breaker is open the scraper should stop writing
`blocked_cooldown` rows, since those 403s are our fault, not the listing's. And
**an auto-restart that fixes the symptom hides the disease**: every automatic
recycle must page, or a weekly restart quietly becomes an hourly one and nobody
notices the solver has stopped working entirely.

### Mechanism: who is allowed to call `docker restart`

Nothing in the stack can restart a container today; `/var/run/docker.sock` is
not mounted anywhere. Plan 108 specified mounting it into `ops` but was never
implemented.

| Option | Verdict |
|---|---|
| Host `systemd` timer | Simplest for Stage 3, useless for Stage 4 (no app signal), and lives outside git — violates the commit/push/pull deployment rule |
| Mount `docker.sock` into `ops` (Plan 108) | Full Docker API access ≈ root on the host, granted to an internet-facing service. Too much authority for one restart |
| **`docker-socket-proxy`, scoped to `POST /containers/*/restart`** | **Recommended.** A tiny sidecar holds the socket; `ops` gets a URL that can do exactly one verb. Restart capability without root-equivalence |
| Airflow `DockerOperator` | Airflow is `LocalExecutor`, so this means the socket in the scheduler — same authority problem, plus it makes Stage 4 depend on the scheduler being healthy |

Recommended shape: `docker-socket-proxy` sidecar → `POST /maintenance/recycle/{service}`
on `ops`, with an allowlist of exactly `["trawl"]`. Stage 3's scheduled recycle
and Stage 4's circuit breaker both call that one endpoint, so there is a single
audited path to a restart. It also lays the groundwork Plan 108 wanted, with far
less authority than Plan 108 proposed.

> Verify: with a deliberately broken solver in staging, the breaker trips within
> ~10 min, exactly one recycle is issued, an alert is sent, no `blocked_cooldown`
> rows are written while the breaker is open, and scraping resumes automatically.

---

## Sequencing

**Stage 0 went first and was separable from the rest of the plan** — 0a a
config change fixing a live production defect, 0c a one-line expression fix,
neither depending on anything else here. Both shipped and were verified on
2026-08-18. 0b's ordering caveat resolved differently than written: rather than
waiting on Stage 4's `docker-socket-proxy`, the whole step moved to Plan 140,
which takes the socket-path decision with it.

**Plan 140 now runs before the remaining work.** It was previously reasoned as
adjacent on switching cost; with 0b folded into it, it is simply the next slice
of the same work, and Stage 0a/0c were the only parts it was waiting on. Plan
143 then establishes analytics freshness at the correct serving boundary before
this plan resumes at Stage 2.

Plan 143 and Stage 2 are worth doing regardless — they are the difference
between finding out in 15 minutes and finding out in 8 hours. **Stage 3 is the highest
value-per-effort item in the plan** and could ship on its own. Stage 4 is the
only one that requires new authority in the stack and should not start until
Stage 2's counters have run long enough to trust the breaker's input signal.

Note that container health and Stage 2 attack the same failure from opposite
ends. Health asks *"is the container healthy?"*; Stage 2 asks *"is work
succeeding?"* Neither subsumes the other — the solver incident had a **healthy**
container producing 0% success, and the apiserver incident had an **unhealthy**
container while statsd-exporter reported normally. Both incidents needed the
signal the other provides.

**That argument survived 0b moving to Plan 140 and is the reason Stage 2 is not
optional afterwards.** Plan 140 makes the liveness floor uniform; it cannot make
`trawl` tell the truth, because `trawl`'s healthcheck returned `status:ok` for
all eight hours. Finishing Plan 140 and reading health coverage as done is the
specific mistake this note exists to prevent.

## Relationship to Plan 141

[Plan 141](plan_141_structured_log_ingestion_contract.md) owns log parsing,
severity/source labels, dashboard selectors, and ingestion-volume acceptance.
It does not block this plan's Prometheus-based solver liveness or Plan 143's
analytics freshness work. Keep these boundaries explicit:

- Revalidate `ct-403-log-spike` against Plan 141's fixtures and deployed labels
  because it is this plan's Loki-dependent alert.
- Warning-only observations added here must query an explicit service and level;
  absence from a dashboard is not evidence that no warning occurred.
- The Airflow HMAC-key-length warning observed during the Plan 135 closeout is a
  configuration defect, not a line for Plan 141 to suppress. Resolve or route it
  with Stage 0's other Airflow configuration work.
- Container health remains in Plan 140, analytics freshness in Plan 143, and
  solver efficacy/restart authority here; Plan 141 must not recreate them from
  logs.

## Files

Stage 0 rows are marked **done**; the container-health producer moved to
[Plan 140](plan_140_service_health_contract.md) Stage 2 and is listed there.

| File | Change | Stage |
|---|---|---|
| `docker-compose.yml` | Apiserver-only `SQL_ALCHEMY_POOL_SIZE` / `MAX_OVERFLOW` | 0a — **done** |
| `tests/test_observability_config.py` | `TestAirflowConnectionBudget`: pool settings present, not on the shared anchor, exact set equality on the Airflow services, worst-case sum < `max_connections` read from the postgres `command:` | 0a — **done** |
| `grafana/provisioning/alerting/rules.yml` | `dag_id != ""` guard on `ct-pipeline-failures` | 0c — **done** |
| [Plan 143](plan_143_analytics_serving_snapshot.md) file set | Saved SQL, durable post-build snapshot, direct `dbt_runner` metrics, fail-loud freshness, and removal of analytics reads from `ops` | Former Stage 1 — **transferred before deployment** |
| `scraper/metrics.py` | Solver + detail-fetch outcome counters, all label children pre-initialized | 2 — **done** |
| `shared/challenge.py` | Interstitial marker set + title reader, shared with `processing` so the two classifiers cannot drift | 2 — **done** |
| `scraper/processors/cf_session.py` | `_solver_outcome` classifier; count every bootstrap outcome, including the raising paths | 2 — **done** |
| `scraper/processors/scrape_detail.py` | Count one outcome per `_fetch_url` call, on both the solver and fallback paths | 2 — **done** |
| `scraper/app.py`, `scraper/requirements.txt` | Expose `/metrics` via `Instrumentator`, as `ops` and `processing` do | 2 — **done** |
| `prometheus/prometheus.yml` | Scrape the `scraper` target | 2 — **done** |
| `grafana/provisioning/alerting/rules.yml` | Add `ct-metrics-freshness` | Former Stage 1 — **Plan 143** |
| `grafana/provisioning/alerting/rules.yml` | `ct-solver-not-solving` + `ct-detail-fetch-failing`; `scraper` added to `ct-service-down`; `ct-scrape-volume-drop` re-annotated as data-quality | 2 — **done** |
| `dbt_runner/sql/analytics_metrics_snapshot.sql` | Exclude the in-progress hour, so the "last complete hour" gauge is one (D1) | 2 — **done** |
| `grafana/dashboards/pipeline_health.json` | Solver and detail outcome-rate panels — the read that answers open question 2 | 2 — **done** |
| `scraper/` (metrics module) | Circuit breaker | 4 |
| `docker-compose.yml` | `docker-socket-proxy` sidecar; `RECYCLABLE_SERVICES` for ops | 4 |
| `ops/routers/maintenance.py` | `POST /maintenance/recycle/{service}`, allowlisted | 3, 4 |
| `airflow/dags/` | Weekly drain-aware recycle DAG | 3 |
| `tests/ops/routers/test_maintenance.py` | Recycle endpoint: happy path, non-allowlisted service, proxy unreachable | 3, 4 |

## Open questions

1. **Why did `ct-scrape-volume-drop` start firing at 05:51 rather than ~03:00,**
   when the gauge had read 44 (below the threshold of 100) since 02:30? Most
   likely the D2 lock-skips intermittently produced `NoData`, and
   `noDataState: OK` reset the 30-minute `for:` window each time. Worth
   confirming, because if that is right then D2 also *suppresses* alerts rather
   than merely staling them.
2. **Does trawl's solve rate decay gradually or fall off a cliff?** Determines
   whether weekly recycling is conservative or lucky. Stage 2 answers it — the
   two outcome-rate panels on the Pipeline Health dashboard are the read, and
   **Stage 3's interval should not be chosen before there is a baseline in
   them.** That is the reason Stage 3 was not built alongside Stage 2 despite
   the build order pairing them.
3. **Should the vestigial `cartracker-flaresolverr` container be removed, and
   `FLARESOLVERR_URL` renamed to `SOLVER_URL`?** Low effort; it cost real time
   during this incident.
4. **Is the apiserver pool undersized, or leaking?** The evidence does not yet
   separate them. Four weeks of stability followed by a wedge the day after two
   DAGs were added points at load growth; a leak would also look like this if it
   were slow enough. **Raising the pool fixes sizing and only delays a leak**, so
   the distinction has to be settled by observation, not by whether 0a appears to
   work. Watch `pg_stat_activity` filtered to `airflow_user` over several days:
   monotonic growth between restarts means leak, a plateau means sizing. Baseline
   was **10** shortly after the 01:45 restart, against a 15-connection ceiling
   before it.

   **Still open after 0a shipped.** Second datapoint: **7**, roughly two minutes
   after the 15:39 recreate on 2026-08-18, now against a 40-connection ceiling.
   Two post-restart readings say nothing about the trend — the question is the
   slope *between* restarts, and 0a raising the ceiling means a leak now has more
   room to hide before it wedges anything. That is the cost of the fix and the
   reason this stays open rather than closing on a green deploy.
5. **Which containers belong in the 0b health alert?** Alerting on every
   container invites noise from short-lived and profile-gated services
   (`snapshot-worker`, `flyway`, `airflow-init`). An allowlist of long-running
   services is probably right, but it has the failure mode that a service added
   later is silently unwatched — which is exactly the class of gap D4 is about.
   Prefer a deny-list of known-transient containers if it can be kept short.
