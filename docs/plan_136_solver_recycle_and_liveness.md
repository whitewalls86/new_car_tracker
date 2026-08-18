# Plan 136: Solver Recycle and Real Liveness — The Alert Fired During the Recovery

## Status

PLANNED. Written 2026-08-15 after an 8-hour detail-scraping outage that no
alert caught. Nothing in this plan has been applied. The only production action
taken during the incident was `docker restart cartracker-trawl`, which resolved
it.

**Extended 2026-08-18 after a second incident** — the Airflow apiserver wedged
on an exhausted connection pool and, again, **no alert caught the failing
component**; a downstream DAG failure raised it seven minutes later. Same shape,
different service, which is why it lives here rather than in its own plan. It
adds D4 and D5 and a new [Stage 0](#stage-0--the-apiserver-fixes-and-container-health-as-a-signal).
Still nothing applied; the only action taken was
`docker restart cartracker-airflow-apiserver`.

Two incidents now share one root cause: **the health of a component is not a
signal this system collects.** Both were found by noticing damage downstream.

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

Root cause is in [ops/metrics/duckdb_gauges.py:55-61](ops/metrics/duckdb_gauges.py#L55-L61):

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

[ops/metrics/duckdb_gauges.py:128-132](ops/metrics/duckdb_gauges.py#L128-L132):

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

## Goal

1. Detect a solver outage in **under 15 minutes**, from a signal that does not
   pass through dbt.
2. Never let a gauge report a stale value as if it were live.
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

---

## Stage 1 — Make gauge staleness impossible to miss

Smallest change, unblocks verification of everything else.

**1a. Fail loudly instead of silently.** On any refresh failure, set the affected
gauges to `float('nan')`. Prometheus records NaN, comparisons against it are
false, and `noDataState` becomes reachable — a skipped refresh stops looking
like a healthy reading.

**1b. Add a freshness gauge.** `cartracker_metrics_last_success_timestamp_seconds`,
set to `time.time()` after a fully successful refresh. Alert on
`time() - cartracker_metrics_last_success_timestamp_seconds > 900`. This makes
"the metrics pipeline itself is broken" a first-class, alertable condition.

**1c. Stop the lock contention at the source.** The maintenance router already
solved this — see the comment at
[ops/routers/maintenance.py:27-31](ops/routers/maintenance.py#L27-L31), which
reads MinIO parquet directly with a fresh S3-configured connection *specifically
to avoid dbt's write lock*. Apply the same approach to the gauges rather than
contending on `analytics.duckdb`.

> Verify: with a dbt build running, `/metrics` returns NaN for affected gauges
> and a stale `last_success_timestamp`, and the freshness alert fires.

## Stage 2 — A liveness signal that does not pass through dbt

Add a **solver outcome counter owned by the scraper**, which already knows every
outcome at the moment it happens:

- `cartracker_solver_requests_total{outcome="ok|challenge|error"}`
- `cartracker_detail_fetch_total{outcome="ok|403|error"}`

Prometheus does not currently scrape the scraper at all — add it to
[prometheus/prometheus.yml](prometheus/prometheus.yml) alongside `ops` and
`processing`.

Then the alert that should have caught this:

```promql
rate(cartracker_solver_requests_total{outcome="ok"}[15m]) == 0
  and rate(cartracker_solver_requests_total[15m]) > 0
```

Read: *we are asking trawl for things and none of them are succeeding.* True
within 15 minutes of 21:00 on the night in question, and immune to both D1 and
D3 — it needs no dbt, and it is a ratio, so a slow failure trips it exactly as
readily as a fast one.

> Verify: replay the incident by pointing the scraper at a deliberately broken
> solver URL in staging; alert fires within 15 min.

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

**Stage 0 goes first and is separable from the rest of the plan.** 0a is a
two-line config change fixing a live production defect; 0c is a one-line
expression fix. Neither depends on anything else here. 0b is the one piece that
should watch its ordering: if Stage 4's `docker-socket-proxy` is close, read
container health through it rather than adding a second socket path.

Stages 1 and 2 are worth doing regardless — they are the difference between
finding out in 15 minutes and finding out in 8 hours. **Stage 3 is the highest
value-per-effort item in the plan** and could ship on its own. Stage 4 is the
only one that requires new authority in the stack and should not start until
Stage 2's counters have run long enough to trust the breaker's input signal.

Note that Stage 0b and Stage 2 attack the same failure from opposite ends. 0b
asks *"is the container healthy?"*; Stage 2 asks *"is work succeeding?"* Neither
subsumes the other — the solver incident had a **healthy** container producing
0% success, and the apiserver incident had an **unhealthy** container while
statsd-exporter reported normally. Both incidents needed the signal the other
stage provides.

## Files

| File | Change | Stage |
|---|---|---|
| `docker-compose.yml` | Apiserver-only `SQL_ALCHEMY_POOL_SIZE` / `MAX_OVERFLOW` | 0a |
| `tests/test_observability_config.py` | Pool settings present; Airflow worst-case connection sum < `max_connections` | 0a |
| container-health producer (textfile `.prom`) | `cartracker_container_health{container=...}` | 0b |
| `grafana/provisioning/alerting/rules.yml` | Container-health alert; `dag_id != ""` guard on `ct-pipeline-failures` | 0b, 0c |
| `ops/metrics/duckdb_gauges.py` | NaN on failure; freshness gauge; S3 connection to dodge the dbt lock | 1 |
| `scraper/` (metrics module) | Solver + detail-fetch outcome counters; circuit breaker | 2, 4 |
| `prometheus/prometheus.yml` | Scrape the `scraper` target | 2 |
| `grafana/provisioning/alerting/rules.yml` | Fix `ct-scrape-volume-drop`; add solver-success and metrics-freshness alerts | 1, 2 |
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
   whether weekly recycling is conservative or lucky. Stage 2 answers it.
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
5. **Which containers belong in the 0b health alert?** Alerting on every
   container invites noise from short-lived and profile-gated services
   (`snapshot-worker`, `flyway`, `airflow-init`). An allowlist of long-running
   services is probably right, but it has the failure mode that a service added
   later is silently unwatched — which is exactly the class of gap D4 is about.
   Prefer a deny-list of known-transient containers if it can be kept short.
