# Plan 136: Solver Recycle and Real Liveness — The Alert Fired During the Recovery

## Status

PLANNED. Written 2026-08-15 after an 8-hour detail-scraping outage that no
alert caught. Nothing in this plan has been applied. The only production action
taken during the incident was `docker restart cartracker-trawl`, which resolved
it.

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

## The three defects

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

Stages 1 and 2 are worth doing regardless — they are the difference between
finding out in 15 minutes and finding out in 8 hours. **Stage 3 is the highest
value-per-effort item in the plan** and could ship on its own. Stage 4 is the
only one that requires new authority in the stack and should not start until
Stage 2's counters have run long enough to trust the breaker's input signal.

## Files

| File | Change |
|---|---|
| `ops/metrics/duckdb_gauges.py` | NaN on failure; freshness gauge; S3 connection to dodge the dbt lock |
| `scraper/` (metrics module) | Solver + detail-fetch outcome counters; circuit breaker |
| `prometheus/prometheus.yml` | Scrape the `scraper` target |
| `grafana/provisioning/alerting/rules.yml` | Fix `ct-scrape-volume-drop`; add solver-success and metrics-freshness alerts |
| `docker-compose.yml` | `docker-socket-proxy` sidecar; `RECYCLABLE_SERVICES` for ops |
| `ops/routers/maintenance.py` | `POST /maintenance/recycle/{service}`, allowlisted |
| `airflow/dags/` | Weekly drain-aware recycle DAG |
| `tests/ops/routers/test_maintenance.py` | Recycle endpoint: happy path, non-allowlisted service, proxy unreachable |

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
