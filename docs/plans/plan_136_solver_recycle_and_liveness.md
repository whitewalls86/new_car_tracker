# Plan 136: Solver Recycle and Real Liveness — The Alert Fired During the Recovery

## Status

**Stage 0 complete and verified in production 2026-08-18. Stage 2 deployed to
production 2026-08-20 (PR #223, merge `50bba68`); its 24-hour soak closed on
2026-08-21 — the alert half green, the shape half inconclusive by construction.
Stage 3 redesigned 2026-08-23; Stage 3a deployed to production and publishing;
Stage 3b blocked until the memory baseline is read; Stage 4 not started.**

The soak proved both new rules quiet and the counters healthy, but it **did not
answer open question 2**: a healthy window contains no solver decay to read, so
at the time Stage 3's recycle interval had nothing to be chosen from. See
[the soak record](#24-hour-soak-record--2026-08-21). **[D7](#d7--the-involuntary-recycle-stopped-and-the-leak-stopped-being-harmless)
(2026-08-22) changed that** — the rate bent at four days, and the involuntary
OOM recycle Stage 3 was sized against has stopped firing.

**[D8](#d8--what-the-recycle-setting-counts-and-why-the-socket-cannot-lend-one-verb)
(2026-08-23) then settled the question D7 left open and took two shortcuts off
the table.** Production runs a **2026-07-06** image while the compose file is
written for one six weeks newer: `BROWSER_RECYCLE_AFTER_CONTEXTS` and
`BROWSER_CONTENT_PROCESSES` are set on the container and read by nothing, and
the running pool has **no periodic recycling of any kind**. The existing
`docker-socket-proxy` cannot lend a single `POST` verb, because `ALLOW_RESTARTS`
narrows nothing once `CONTAINERS=1` is set. And nothing in the stack publishes
the memory headroom D7 says to size against.

[Stage 3](#stage-3--scheduled-recycle) is now four slices — a memory series, a
pinned image upgrade that may end the stage outright, a narrow second proxy
instance, and a threshold-gated recycle holding an Airflow pool — replacing the
weekly `docker restart` and its hand-built drain protocol. **3a is built** and
deploys with two Infrastructure dashboard panels; 3b onward are not started.

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

**Extended a fourth time 2026-08-22 with [D7](#d7--the-involuntary-recycle-stopped-and-the-leak-stopped-being-harmless),
which breaks that streak in the right direction.** A second `trawl` outage was
caught by `ct-solver-not-solving` about 30 minutes in, not by a human hours
later — Stage 2 working as designed, and **Goal 1 met in production**. But it
failed at **4 days** of uptime, not 22, by **memory exhaustion** rather than
state rot, and with an **unhealthy** container rather than a healthy one. The
load-bearing finding is that the `CONSTRAINT_MEMCG` kill Stage 3 assumes is
recycling the browsers every 1.5-4 days **has not fired once in 4.5 days**. The
involuntary recycle this plan was designed around is gone, which changes what
Stage 3 must be sized against.

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

**The restart was incomplete, and the gap held for another day.** Only the
scheduler was restarted. `airflow-dag-processor` (up since 04:58:36),
`airflow-triggerer` (04:24:33) and `airflow-apiserver` (15:39:41) all predate
the 17:03:06 exporter recreate and kept sending into the void until
**2026-08-21 17:21**, when Plan 144's read-only sweep found them by comparing
container start times against the exporter's.

Their metrics were absent, not renamed: point-querying before the recreate as a
known-good control returned `airflow_dag_processing_processes` = 3,416,623 and
a present `airflow_triggers_running`, both of which had since gone to an empty
vector. After
`redeploy.sh --restart airflow-dag-processor airflow-triggerer airflow-apiserver`
they read 18 and 0 respectively — a series again rather than nothing.

Recorded in [Plan 144](plan_144_deploy_script_hardening.md); the lesson is that
"restart the senders" is a *set* operation, and the set is every long-lived
process inheriting `STATSD_HOST` from `x-airflow-common`, not the one whose
panel someone happened to be looking at.

Still owed:

1. **`ct-pipeline-failures` must treat NoData as a failure.** For a metric that
   should always be present, `noDataState: OK` is the defect. Plan 143 already
   set this precedent with `ct-metrics-freshness`.
2. **A staleness signal for the Airflow scrape**, since `up` cannot see this.
   It read 1 throughout both the original outage and the extra day.

Item 3 — *a deploy-time check for the class* — **is done.** Plan 144 absorbed
it: `deploy-followers.txt` names the services whose peers cache their address,
and a recreate prints the entry with the exact restart command rather than
leaving it to an operator's memory. `promtail` and `postgres-exporter` were
audited and are not exposed: both talk TCP, so a recreated peer produces a
visible connection error and they re-resolve. UDP is the whole hazard.

Items 1-2 are why this is a defect and not just an incident log: the restart
fixes today, and nothing yet would catch the next one *silently going*.

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

## D7 — The involuntary recycle stopped, and the leak stopped being harmless

**Found 2026-08-22 by `ct-solver-not-solving` — the first of this plan's
findings that an alert caught rather than a human noticing damage downstream.
Stage 2's counters worked exactly as designed.**

| | |
|---|---|
| Started | 2026-08-22 ~17:17 UTC |
| Detected | 2026-08-22 ~17:47, **by `ct-solver-not-solving`**, ~30 min in |
| Resolved | 2026-08-22 17:52:56 UTC, `docker restart cartracker-trawl` |
| Duration | ~35 minutes |
| Cost | One partial batch degraded to `curl_cffi` fallback; detail fetches held ~98.9% `ok` (796 vs 9 403s) across the window |

Detection is the headline. The 2026-08-14 outage ran 8h 12m before a human
noticed; this one was on a screen in about half an hour, from a signal that does
not pass through dbt. **Goal 1 is met in production.**

### A third failure shape, distinct from both known ones

The plan has documented two solver shapes: *refusing* (`outcome=error`) and
*lying* (`outcome=challenge`, status ok behind an interstitial). This was the
refusing shape — 100% `error`, **0** `challenge` — but reached by a mechanism
neither shape describes, and with the opposite container signal:

| | 2026-08-14 | 2026-08-22 |
|---|---|---|
| Container status | **healthy** for all 8h | **unhealthy**, failing streak 53 |
| Uptime at failure | 22 days | **4 days** |
| Mechanism | container-level state rot; `/v1` 500s | **memory exhaustion**; 429 + 135s read timeouts |
| Solve rate before | degraded to 0% | flat 2.2-3.0/hour through 17:00, then cliff |

Pool browser PID 23, alive since the Aug 18 boot, held **3.18 GB RSS** against
the 4 GB `mem_limit`; the container sat at **99.57%**. Both pool parents and the
`bun` API were in `D` state on page reclaim. `BROWSER_POOL_SIZE` is 2, so with
both browsers wedged every acquire blew past `BROWSER_ACQUIRE_TIMEOUT_MS` —
which is why the scraper saw 429s and 135s read timeouts, and why the 10s
healthcheck timed out. One cause, every symptom. The restart took memory to
727 MiB with the pool at 2/2 and solves resuming within ~20 seconds.

### The finding that matters: the OOM killer did not fire

Stage 3's design blockquote rests on a premise that is **no longer true**. It
argues that `camoufox-bin` is OOM-killed every 1.5-4 days, that the browsers are
therefore *already* being recycled involuntarily far more often than weekly, and
that Stage 3's interval should be chosen against whatever container-level state
survives those kills.

That involuntary recycle has stopped:

| Boot | Window | `CONSTRAINT_MEMCG` kills |
|---|---|---|
| previous | 2026-07-31 - 2026-08-18 (~18 days) | **3** (Aug 05, 11, 15) |
| current | 2026-08-18 04:22 - 2026-08-22 (4.5 days) | **0** |

The journal covers the whole current boot, so the zero is real and not a
retention artifact. The browser reached 3.18 GB — squarely inside the
~3.2-3.5 GB band that used to get it killed — and **was not killed**. It
thrashed at the cgroup ceiling instead.

**This inverts the guardrail's role.** Plan 124's cap was doing the containing
it was built to do, and the kill it used to produce was, in effect, a free
recycle that kept the leak harmless. Without the kill, the same leak stops being
"expected background noise" (the runbook's words) and becomes an outage that
only a manual restart clears.

Whether the Aug 18 reboot onto kernel `6.8.0-1058-oracle` caused this or it is
coincidence is **not established** — one missed kill against a ~5-6 day
historical interval is suggestive, not proof. The next kill, or a second clean
4-day window, settles it. That question should not block acting on the rest.

### What this does to Stage 3

Two things, both material:

1. **There are two failure clocks, not one.** A ~22-day container-state clock
   and a ~4-day memory clock. The weekly cadence in Stage 3 was sized against
   the 22-day figure with "3x margin" — **it would not have prevented this
   outage**, which arrived on day 4.
2. **The interval can no longer be chosen against uptime alone.** The
   blockquote was already right that uptime is the wrong axis; this sharpens it.
   Memory headroom is a directly observable input, and unlike 22-day state rot
   it does not require weeks of waiting to read.

Separately, `BROWSER_RECYCLE_AFTER_CONTEXTS=8` plainly did not recycle PID 23
across four days. Whether that setting is ineffective, or counts something other
than what its name implies, is worth establishing before an interval is chosen —
if in-container recycling can be made to work, it is cheaper than a scheduled
`docker restart` and needs no new socket authority.

### Partial answer to open question 2

OQ2 asks whether the solve rate decays gradually or falls off a cliff. On this
mechanism it is **unambiguously a cliff**: flat 2.2-3.0/hour through 17:00, then
100% `error` from 17:17, with no intermediate degradation in any five-minute
bucket. That is one datapoint on the *memory* clock. It says nothing about the
22-day state-rot clock, which remains unobserved since 2026-08-14.

## D8 — What the recycle setting counts, and why the socket cannot lend one verb

**Found 2026-08-23 while sizing Stage 3, by reading the solver image's own
source rather than by watching production.** Four findings now. Two invalidate
sentences written elsewhere in this plan, one is the reason Stage 3 could not
have been sized on the day D7 unblocked it, and the first was found by checking
the others against the running container instead of against `:latest`.

### The image in production is not the image the compose file is written for

`TRAWL_IMAGE` defaults to `ghcr.io/germondai/trawl:latest`, but a tag is
resolved at pull time and `restart: unless-stopped` never pulls. The container
running on 2026-08-23 was created from an image built **2026-07-06**
(`sha256:d4d7beb2…`). `:latest` that same day resolved to a build from
**2026-08-21** (`sha256:86b1fdf2…`) — six weeks apart, and the browser pool grew
from 249 lines to roughly 750 between them.

So the compose file has been configured against a solver we do not run. Of the
five environment variables set on `trawl`, the running image reads three:

| Set in `docker-compose.yml` | Read by the running image |
|---|---|
| `BROWSER_POOL_SIZE=2` | yes |
| `BROWSER_ACQUIRE_TIMEOUT_MS=30000` | yes |
| `SESSION_TTL_SECONDS=3600` | yes |
| `BROWSER_RECYCLE_AFTER_CONTEXTS=8` | **no — the variable does not exist in it** |
| `BROWSER_CONTENT_PROCESSES=2` | **no — same** |

Enumerated from the image itself (`grep -rhoE "process\.env\.[A-Z_]+"` over
`/app`), not inferred. The second inert variable matters on its own: it is the
knob that caps Firefox content processes per browser, a memory-relevant
setting we believed was holding at 2 and which has never been read.

**And the July pool has no periodic recycling of any kind.** Its only restart
path fires when `browser.isConnected()` returns false — "disconnected,
restarting". There is no rolling replacement, no temporary-context counter, no
stall detection. A browser and its persistent context are created at startup
and live until the process dies. Nothing in the running image could ever have
bounded memory growth, which is why the `CONSTRAINT_MEMCG` kill was
load-bearing and why removing it produced D7 directly.

Two production reads on 2026-08-23 19:07 UTC confirm it, ~25 hours after the
D7 restart: `GET /stats` reports `restarts: 0`, and `trawl`'s logs contain no
pool restart line at all. Memory was **1.315 GiB** against a 727 MiB
post-restart baseline — roughly **590 MiB/day**, which reaches D7's 3.18 GB
wedge point at about **4.3 days** and matches the 4-day failure almost exactly.

> **The lesson is the method, not the tag.** These findings were first read
> from `:latest` and were wrong about production for that reason. Read the
> digest the container is actually running (`docker inspect --format
> '{{.Image}}'`) before concluding anything about behaviour, and pin
> `TRAWL_IMAGE` to a digest so the compose file and the running code cannot
> drift again.

### What the recycle setting will do once we are current — prospective

**This subsection describes the 2026-08-21 build, not production.** It was
written before the drift above was found, and it is kept because
[3b](#3b--get-current-on-a-pinned-digest) moves us onto exactly this build, at
which point it becomes live.

D7 left this as the question to settle before an interval is chosen: is the
setting ineffective, or does it count something other than what its name
implies? In the newer build it is the second, and upstream says so in the
config file it ships:

> Rolling-replace a browser after this many **Tier 3/4 temporary contexts**.
> Every creation counts regardless of outcome; 0 disables periodic replacement.
>
> — `/app/apps/api/src/config.ts`, `ghcr.io/germondai/trawl@sha256:86b1fdf2…`

The counter has exactly two call sites, `tiers/3.ts` and `tiers/4.ts`, both
passing `onCreated: handle.noteTemporaryContext` into `newFreshContext`. Which
tier serves a request decides whether anything is counted at all:

| Tier | What it is | Browser | Counts toward recycle |
|---|---|---|---|
| 1 | plain HTTP fetch | none acquired | no |
| 2 | cached Redis session replayed into the **persistent** context | pooled | **no** |
| 3 | fresh temporary context, solves the challenge | pooled | yes |
| 4 | tier 3 through a residential proxy | pooled | yes |

The persistent context tier 2 reuses is exactly where the leak accumulates, and
the pool says so on purpose — `release()` keeps it alive because "CF cookies
(`cf_clearance`, `__cf_bm`) and browser cache accumulate, making subsequent
challenges faster." **The setting counts the path that is rebuilt anyway and
ignores the one that grows.**

Our traffic makes that worse rather than better. `get_cf_credentials` reaches
`/v1` only on a 25-minute cache miss, D7 measured 2.2–3.0 solves/hour, and each
of those is split again by whether `trawl`'s own Redis session (TTL 3600s) is
warm. `pickEntry` is domain-sticky, so one pool entry absorbs every cars.com
request — which is why D7 found **one** browser at 3.18 GB rather than two at
1.6.

Stated precisely, because the useful version is narrower than "it is broken":
the setting cannot bound the leak *as a function of leaked memory*, because it
is driven by a counter uncorrelated with the leaking path. A rolling
replacement does rebuild the whole browser, persistent context included, so
when it fires it clears the leak as a side effect. It simply has no reason to
fire when traffic is served from cache. That is what makes
[3b](#3b--ask-the-in-container-recycle-first) an experiment worth running and
not a fix worth assuming.

**There is a free read that settles it empirically, on either build.**
`GET /stats` on `trawl` exposes `restarts`, the sum of `restartCount` across
pool entries. On the current image only a disconnect increments it; on the
newer one, rolling replacements do too. Either way `restarts == 0` across a
boot is standalone proof that nothing recycled, with no log archaeology — and
it read 0 at 25 hours on 2026-08-23.

### The socket proxy cannot lend one verb

[PLANS.md](../PLANS.md) records the `POST` verb as separable, "as one added verb
on the existing `docker-socket-proxy` grant rather than a second socket path."
The proxy's own config refuses that shape. Its rules are evaluated in order:

```
http-request deny unless METH_GET || { env(POST) -m bool }
http-request allow if { path ... /containers/[id]/((stop)|(restart)|(kill)) } { env(ALLOW_RESTARTS) -m bool }
...
http-request allow if { path ... ^(/v[\d\.]+)?/containers } { env(CONTAINERS) -m bool }
http-request deny
```

`ALLOW_RESTARTS` narrows nothing once `CONTAINERS=1` is set; the broader rule
below it allows the request anyway. So:

| Grant | What it permits | Verdict |
|---|---|---|
| `CONTAINERS=1`, `POST=0` | every GET under `/containers` | today's exporter |
| `CONTAINERS=1`, `POST=1` | **every POST under `/containers`, including `/containers/create`** | a privileged container with `/` mounted — root on the host |
| `CONTAINERS=0`, `POST=1`, `ALLOW_RESTARTS=1` | `stop`, `restart`, `kill` on a container, and nothing else | what Stage 3 needs |

The read grant and the restart grant are mutually exclusive **within one
instance**. The exporter needs `CONTAINERS=1` for its GETs; adding restart
authority to that instance grants container creation, which is the exact
authority Plan 140 rejected `docker.sock` over.

The resolution is a second **proxy instance**, not a second socket path — the
distinction the existing warning was drawing. A `docker-socket-proxy-restart`
sidecar with `CONTAINERS=0, POST=1, ALLOW_RESTARTS=1`, on its own `internal`
network with `ops` as the only other member, still holds the socket behind a
proxy and still hands out exactly one capability. Nothing new can reach the
daemon; a second, strictly narrower door reaches a different part of it.

`test_socket_access_stays_confined_to_the_proxy` asserts the socket holders are
`{docker-socket-proxy, promtail}` and must be updated to name both proxies with
their distinct grants. That is the assertion doing its job, not an obstacle to
route around.

### There is no memory series, and the number in the runbook is a sawtooth midpoint

D7 says the interval should be re-derived "against memory headroom rather than
the 22-day uptime figure." Nothing publishes that headroom. `node-exporter` is
host-level and `container-health` publishes health alone, so D7's own
measurements came from a human running `docker stats` during an incident.

The one number written down is worse than absent, because it reads as a
baseline: [the runbook](../runbooks/runbook_solver_oom_and_recycle.md) records
steady state at "around **70%** of the 4 GB cap" with a kill likely above ~85%.
That figure was measured while `CONSTRAINT_MEMCG` kills fired every 1.5–4 days.
**It is the midpoint of a sawtooth D7 established has stopped.** Against a
monotonic climb it describes nothing, and the two points actually in evidence —
an old cycling average and 3.18 GB at the moment of the wedge — do not
distinguish a curve that plateaus at 2.8 GB from one that climbs steadily to
3.2 and thrashes.

That distinction decides whether a 3 GB threshold gives three days of warning or
fifteen minutes, and both ways of being wrong are silent: too high never fires,
too low recycles every fifteen minutes. Hence
[3a](#3a--a-memory-series-to-size-against) before anything is gated on memory.

## Goal

1. Detect a solver outage in **under 15 minutes**, from a signal that does not
   pass through dbt.
2. Never let a gauge report a stale value as if it were live. Plan 143 owns the
   analytics-serving implementation and freshness alert that satisfy this goal.
3. Recycle `trawl` on a schedule so neither 22-day state rot nor the ~4-day
   memory leak of [D7](#d7--the-involuntary-recycle-stopped-and-the-leak-stopped-being-harmless)
   can accumulate.
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
[prometheus/prometheus.yml](../../prometheus/prometheus.yml) alongside `ops` and
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
set moved to [shared/challenge.py](../../shared/challenge.py) so `processing`'s
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

Restart `trawl` before its memory reaches the band where the pool wedges, in a
window where no scrape can be harmed by it.

**The gotcha that makes this non-trivial** survives every revision: a naive
restart mid-batch fails every in-flight request, and the scraper's 403 handler
pushes each of those listings into a 12-hour cooldown. A careless recycle
inflicts a small version of the very outage it prevents.

What has *not* survived is almost everything else. D7 removed the 22-day clock
this stage was sized against; D8 removed the cheap in-container alternative it
hoped for and the one-verb socket grant it assumed. The
[original reasoning is kept below](#superseded-reasoning-kept-for-the-record)
rather than deleted, because the way it was wrong is the useful part.

Four slices. Each one is shippable on its own, and each produces the input the
next one needs.

### 3a — A memory series to size against

Publish `cartracker_container_memory_bytes` and
`cartracker_container_memory_limit_bytes` from the existing `container-health`
exporter, read from `GET /containers/{id}/stats?stream=false&one-shot=true`.

**This adds no authority.** That path is a GET under `/containers`, already
inside the `CONTAINERS=1` grant Plan 140 deployed, and needs no new
credential — the exporter's environment stays `{DOCKER_API_URL,
COMPOSE_PROJECT}` and `test_the_exporter_holds_no_other_credentials` stays green
without being edited. `one-shot=true` is load-bearing: without it the daemon
collects two samples a second apart to compute CPU deltas, and this exporter
computes at scrape time against a 15s interval.

Sample only the three services that declare a `mem_limit` — `trawl` (4 GB),
`redis-trawl` (512 MB), `dbt_runner` (12 GB) — rather than the whole fleet, so
the scrape stays cheap and every series has a limit to be read against. That
third one is a bonus: Plan 123's peak-RSS-against-the-8-GB-budget verification
has been open and unmeasured since 2026-07-10 for want of exactly this metric.

> **Verify:** both series appear for all three containers with plausible values
> against `docker stats`; the exporter's scrape duration stays well inside 15s;
> and after ~48 hours the `trawl` series shows a *shape* — plateau or climb —
> which is the read D7 asked for and D8 says nobody has.

### 3b — Get current, on a pinned digest

Move `trawl` from the 2026-07-06 build to a current one, pinned by digest
rather than by `:latest`, and soak for 48 hours against 3a's series.

This replaces the original 3b — "set `TRAWL_BROWSER_RECYCLE_AFTER_CONTEXTS=1`
and watch" — which
[D8](#the-image-in-production-is-not-the-image-the-compose-file-is-written-for)
showed would do exactly nothing: the variable is not read by the image we run.

The newer build reads like scar tissue from our own failure. Against a pool
whose only recovery path is "browser disconnected, restarting", it adds rolling
replacement, a stall detector that reclaims a checkout wedged past its budget,
bounded close and launch timeouts for a Camoufox that hangs on either, and
abandoned-launch accounting so a browser that cannot start stops having
attempts piled on it. D7's failure — both browsers in `D` state, every acquire
blowing past its timeout, the healthcheck timing out behind them — is close to
a description of what those additions exist to survive.

**Pin the digest.** `:latest` is how a six-week drift went unnoticed, and an
unpinned tag on the component that must not break means the next `docker
compose up` is an unreviewed upgrade. `TRAWL_IMAGE` takes a
`ghcr.io/germondai/trawl@sha256:…` reference, and moving it becomes a commit
someone can see.

Read the result three ways: 3a's memory series should stop climbing
monotonically, `GET /stats` `restarts` should leave 0, and the Stage 2 solver
outcome counters should be unchanged — a build that clears memory by degrading
the solve rate is not a fix. `BROWSER_CONTENT_PROCESSES=2` also becomes live
for the first time, so some of any improvement is that variable finally being
read; the two are not separable in one soak and do not need to be.

Know the risks before running it, because this is a bigger change than the
config flag it replaces. Six weeks of upstream change lands at once on the one
component whose failure takes detail scraping down, the two known failure
shapes both live here, and Stage 2's counters are the only instrument that
would catch a regression — which is an argument for doing this *after* 3a is
deployed and has a baseline, not before. Rollback is repinning the old digest,
which is why the pin matters more than the upgrade.

**If 3b holds, Stage 3 may be finished here** and 3c/3d are not built. That is
the outcome to hope for, and the reason this slice runs before the one that
needs new authority in the stack. If the newer pool merely slows the climb
rather than bounding it, the recycle counter becomes the fallback the
[prospective section of D8](#what-the-recycle-setting-will-do-once-we-are-current--prospective)
describes, and 3c/3d proceed as written.

### 3c — Restart authority: a second proxy instance, not a second verb

Per [D8](#the-socket-proxy-cannot-lend-one-verb), the existing proxy cannot
carry this. Add `docker-socket-proxy-restart` with `CONTAINERS=0, POST=1,
ALLOW_RESTARTS=1`, on its own `internal: true` network whose only other member
is `ops`. The socket stays behind a proxy; the new door is strictly narrower
than the existing one, not wider.

`POST /maintenance/recycle/{service}` on `ops`, with an allowlist of exactly
`["trawl"]`, is the single audited path to a restart — 3d's schedule and Stage
4's circuit breaker both call it. Guard the handler with
`single_flight("solver_recycle")` from `shared/job_counter`, which refuses
rather than waits, so a retried HTTP call after a dropped connection returns 409
instead of stacking a second restart onto a container that is already
restarting.

**`restart`, not recreate.** Both the 2026-08-14 and 2026-08-22 outages cleared
with a plain `docker restart` and no image pull. A true recreate would need
`POST /containers/create`, which is precisely the root-equivalent verb this
grant excludes — so restart-only is not a compromise forced by the proxy, it is
sufficient on the evidence.

> **Verify:** the endpoint restarts `trawl` and returns only after `/health`
> reports both browsers warm; a non-allowlisted service is refused without
> reaching the proxy; a second concurrent call returns 409; and the restart
> proxy refuses `GET /containers/json` and `POST /containers/create` while the
> read proxy refuses the restart.

### 3d — The recycle itself: an Airflow pool, not a drain protocol

A `recycle_solver` DAG on `*/15` with one task, which restarts `trawl` when
memory says to and does nothing otherwise.

**The exclusive window comes from an Airflow pool, which is global across
DAGs** — so the two scrape DAGs keep their own schedules, guards, timeouts and
`max_active_runs=1`, and nothing is merged:

| Task | Pool | Slots |
|---|---|---|
| `scrape_detail_pages.scrape_detail` | `solver` | 1 |
| `scrape_listings.run_scrapes` | `solver` | 1 |
| `recycle_solver.recycle` | `solver` | **2** |

The scheduler will not start the recycle until both slots are free, and will not
start either scrape while the recycle holds both. That is mutual exclusion
declared once, rather than a drain protocol built by hand — and it is why the
recycle does not need to live inside `scrape_detail_pages` as a first or last
task, where the window would only ever be approximated.

The gate is memory, not time: recycle when `trawl`'s 3a series crosses a
fraction of its limit, with the threshold read off the curve 3a produces rather
than guessed now. Memory is self-scaling in the way a fixed interval is not — a
slower leak fires it less often, a faster one more — which is the correction D7
made to the weekly cadence, applied properly.

Two guards on the gate, both in this plan's own idiom:

- **Absent reading means do not recycle *and* alert.** D2 in this plan is
  precisely about a gauge whose stale value was read as truth; a memory gate
  that silently stops firing because its input vanished is the same defect
  wearing a different hat.
- **Alert when the recycle has not fired in N hours.** Airflow does not reserve
  pool slots, so a two-slot task runs only when both happen to be free at once.
  With detail firing every 15 minutes for a few minutes and listings doing real
  work roughly every 4 hours, both-free is the common case — but the failure
  mode of a starved gate is silence, which is the same failure mode
  `BROWSER_RECYCLE_AFTER_CONTEXTS` has been in for four days without anyone
  noticing.

Keep the scraper's `active_jobs == 0` check inside the task as well. It costs
one call to an endpoint that already exists, and it covers anything driving the
scraper from outside Airflow, which the pool cannot see.

> **Verify:** with the threshold temporarily lowered, the recycle fires once,
> restarts `trawl`, and both scrape DAGs queue behind it rather than failing;
> no `blocked_cooldown` rows are written across the window; the memory series
> drops and resumes climbing; and with the threshold restored the gate goes
> quiet without the "has not fired" alert firing spuriously.

### What is deliberately not built

- **No pause/resume drain protocol.** The pool is the drain. The earlier design
  — pause claiming, poll `/scrape_results/jobs/completed`, restart, resume —
  was building by hand what the scheduler already enforces.
- **No unified scrape DAG.** Merging the two would put a detail batch behind a
  listings run that can hold its slot for up to its 2-hour timeout, stalling
  15-minute detail scraping for the duration of every ~4-hourly SRP crawl. The
  pool delivers the exclusivity without the serialization.
- **No recreate, and no host `systemd` timer.** The first needs an authority
  this plan refuses; the second lives outside git and violates the
  commit/push/pull deployment rule.

### Superseded reasoning, kept for the record

> Cadence: weekly. Uptime at failure was 22 days, so weekly carries a 3×
> margin. This is a judgment call, not a measured optimum — Stage 2's counters
> will show whether solve rate degrades gradually (tighten it) or falls off a
> cliff (leave it).

> **Superseded in part by [D7](#d7--the-involuntary-recycle-stopped-and-the-leak-stopped-being-harmless)
> (2026-08-22): the involuntary recycle this rests on has stopped — zero
> `CONSTRAINT_MEMCG` kills in 4.5 days against 3 in the prior 18. Its
> conclusion (uptime is the wrong axis) still holds and is now sharper; its
> premise (the browsers are being recycled for free) does not. A weekly cadence
> would not have prevented the 08-22 outage, which arrived on day 4.**
>
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

> **Superseded again by [D8](#d8--what-the-recycle-setting-counts-and-why-the-socket-cannot-lend-one-verb)
> (2026-08-23).** The blockquote above closes on "the interval should be chosen
> against container-level state, not uptime." Both halves are now answerable and
> neither is an interval: the state that matters is a browser's persistent
> context, and the axis that matters is its memory footprint, which 3a makes
> directly observable. Choosing *any* fixed interval was the wrong move; the
> gate is a threshold.

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

**[3c](#3c--restart-authority-a-second-proxy-instance-not-a-second-verb) owns
this now** — Stage 4 consumes the endpoint it builds rather than specifying its
own. The options table is kept because the rejected rows are still the reasons,
and one of the verdicts has changed.

| Option | Verdict |
|---|---|
| Host `systemd` timer | Simplest for Stage 3, useless for Stage 4 (no app signal), and lives outside git — violates the commit/push/pull deployment rule |
| Mount `docker.sock` into `ops` (Plan 108) | Full Docker API access ≈ root on the host, granted to an internet-facing service. Too much authority for one restart |
| One `docker-socket-proxy`, scoped to `POST /containers/*/restart` | ~~Recommended~~ — **impossible as written.** [D8](#the-socket-proxy-cannot-lend-one-verb): `ALLOW_RESTARTS` narrows nothing once `CONTAINERS=1` is set, so granting `POST` to the exporter's instance grants `POST /containers/create` — root on the host, the authority Plan 140 rejected `docker.sock` over |
| **A second `docker-socket-proxy` instance, `CONTAINERS=0, POST=1, ALLOW_RESTARTS=1`** | **Recommended.** Strictly narrower than the grant already deployed: `stop`, `restart`, `kill`, and nothing else. Still a proxy holding the socket, so this is a second *door*, not a second socket path |
| Airflow `DockerOperator` | Airflow is `LocalExecutor`, so this means the socket in the scheduler — same authority problem, plus it makes Stage 4 depend on the scheduler being healthy |

Both Stage 3's threshold recycle and Stage 4's circuit breaker call the one
`POST /maintenance/recycle/{service}` endpoint, so there is a single audited
path to a restart. It also lays the groundwork Plan 108 wanted, with far less
authority than Plan 108 proposed.

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
between finding out in 15 minutes and finding out in 8 hours. **Stage 3 is the
highest value-per-effort item in the plan** and could ship on its own.

**Within Stage 3 the order is not arbitrary.** 3a comes first because every
later slice reads its series — 3b's pass/fail, 3d's gate, and the threshold
itself. 3b comes before 3c because it may end the stage without new authority
at all, and building the proxy first would bias that read. 3c is the only slice
that expands what the stack is allowed to do, and Stage 4 now inherits it rather
than specifying its own; Stage 4 should still not start until Stage 2's counters
have run long enough to trust the breaker's input signal.

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
| `container_health/docker_api.py` | `container_stats()` — a GET already inside the deployed grant | 3a — **done** |
| `container_health/collector.py` | `memory_capped()` / `memory_usage()` and two gauge families; membership derived from `HostConfig.Memory`, never listed | 3a — **done** |
| `tests/test_container_health_collector.py` | Scoping, the `docker stats` arithmetic against a production sample, and the asymmetry that a stats failure must not blind the health metric | 3a — **done** |
| `grafana/dashboards/infrastructure.json` | "Container Memory Headroom" and "Solver Memory Against Its Cap", under the container-health block | 3a — **done** |
| `tests/test_observability_config.py` | Both series charted; no panel overlaps after the gridPos shift | 3a — **done** |
| `docker-compose.yml` | `TRAWL_IMAGE` pinned to a current `@sha256:` digest; drop the two inert env vars | 3b — revert by repinning the old digest |
| `docker-compose.yml` | `docker-socket-proxy-restart` (`CONTAINERS=0, POST=1, ALLOW_RESTARTS=1`) on its own internal network; `RECYCLABLE_SERVICES` for ops | 3c |
| `tests/test_observability_config.py` | `test_socket_access_stays_confined_to_the_proxy` updated to name both proxies and assert their distinct grants | 3c |
| `ops/routers/maintenance.py` | `POST /maintenance/recycle/{service}`, allowlisted, `single_flight`-guarded | 3c |
| `tests/ops/routers/test_maintenance.py` | Recycle endpoint: happy path, non-allowlisted service, concurrent call → 409, proxy unreachable | 3c |
| `airflow/dags/recycle_solver.py` | `*/15` threshold-gated recycle, `pool="solver", pool_slots=2` | 3d |
| `airflow/dags/scrape_detail_pages.py`, `scrape_listings.py` | `pool="solver", pool_slots=1` on the two solver-consuming tasks | 3d |
| `grafana/provisioning/alerting/rules.yml` | Memory-gate staleness, and recycle-has-not-fired-in-N-hours | 3d |
| `scraper/` (metrics module) | Circuit breaker | 4 |

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

   **Partially answered 2026-08-22 by [D7](#d7--the-involuntary-recycle-stopped-and-the-leak-stopped-being-harmless).**
   On the *memory* clock it is a **cliff**, not a decay — flat 2.2-3.0/hour,
   then 100% `error` from one bucket to the next. That is one datapoint on a
   mechanism the question was not originally about; the 22-day state-rot clock
   is still unobserved since 2026-08-14. The practical consequence is that
   "weekly is conservative" is now known to be **false** for at least one
   failure mode.

   **Closed as a blocker 2026-08-23 by [D8](#d8--what-the-recycle-setting-counts-and-why-the-socket-cannot-lend-one-verb).**
   The question was load-bearing only because Stage 3 needed an interval, and
   [3d](#3d--the-recycle-itself-an-airflow-pool-not-a-drain-protocol) no longer
   chooses one — the gate is a memory threshold. The decay shape stays
   interesting and stays unobserved on the 22-day clock; it no longer blocks
   anything.
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

6. **What is the actual shape of `trawl`'s memory curve, and therefore the
   threshold?** [3a](#3a--a-memory-series-to-size-against) exists to answer
   this and nothing in Stage 3 can be gated on memory until it has. The two
   points on record — the runbook's "around 70%" and D7's 3.18 GB at the wedge
   — do not separate a plateau from a monotonic climb, and per
   [D8](#there-is-no-memory-series-and-the-number-in-the-runbook-is-a-sawtooth-midpoint)
   the first of those is a sawtooth midpoint measured under a kill that has
   stopped firing. **Do not carry the 70% figure into a threshold.**

7. **Did the Aug 18 reboot onto kernel `6.8.0-1058-oracle` stop the
   `CONSTRAINT_MEMCG` kills, or is the clean window coincidence?** Raised in
   [D7](#the-finding-that-matters-the-oom-killer-did-not-fire) and still
   unsettled — one missed kill against a ~5-6 day historical interval is
   suggestive, not proof. The next kill, or a second clean 4-day window, settles
   it. It should not block Stage 3 either way: if the kills resume, the leak
   goes back to being contained for free and the threshold recycle simply stops
   firing, which is the correct behaviour rather than a wasted build.

8. **Will the two-slot pool starve?** Airflow does not reserve slots, so
   `recycle_solver` runs only when both `solver` slots are free at once. The
   arithmetic says this is the common case — detail runs a few minutes out of
   every fifteen, listings does real work about every four hours — but
   arithmetic is not observation, and the failure mode is silence. The
   "has not fired in N hours" alert in
   [3d](#3d--the-recycle-itself-an-airflow-pool-not-a-drain-protocol) exists to
   convert that into a page; if it fires repeatedly, the answer is a priority
   weight or a dedicated window, not a wider pool.
