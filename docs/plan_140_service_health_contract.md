# Plan 140: The Service Health Contract — Coverage That Cannot Silently Rot

## Status

**STAGES 1 AND 3 COMPLETE AND VERIFIED; STAGE 2 IS THE NEXT EXECUTABLE SLICE.**
[PR #216](https://github.com/whitewalls86/new_car_tracker/pull/216) merged the
implementation as `821a6a6`, adding eighteen healthchecks and taking configured
coverage from 7 of 31 services to 25 of 31. The six remaining services are the
five deliberate one-shot/profile exemptions plus `oauth2-proxy`, whose current
distroless image cannot execute a probe. The immediate production gate passed:
all 25 configured runtime checks were healthy with zero failing streaks. **The
24-hour soak closed clean on 2026-08-20** — see the soak record below. Stage 3's
fail-loud CI contract is complete and verified. Stage 2's metric and alerts have
not started, and the soak surfaced a scoping hazard they must handle.

This plan exists because the previous two were each correct and each too narrow.
[Plan 135](plan_135_storage_observability.md) made two disks visible.
[Plan 136](plan_136_solver_recycle_and_liveness.md) makes one solver and one
apiserver visible. Both are enumerations, and **the recurring defect is the
enumeration itself.**

## The measurement that motivated this

Taken from `docker-compose.yml`, 2026-08-18:

| | count | services |
|---|---:|---|
| **With** a healthcheck | **6** | `postgres`, `minio`, `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer` |
| **Without** | **20** | `ops`, `scraper`, `processing`, `archiver`, `pack-worker`, `caddy`, `grafana`, `loki`, `promtail`, `prometheus`, `oauth2-proxy`, `dashboard`, `dbt_runner`, `pgadmin`, `node-exporter`, `postgres-exporter`, `statsd-exporter`, `flaresolverr`, `flyway`, `airflow-init` |
| Profile-gated, **with** a healthcheck | 1 | `trawl` |
| Profile-gated, without | 4 | `redis-trawl`, `dbt`, `dbt_test`, `snapshot-worker` |

Thirty-one services, seven healthchecks.

**Docker reports no health status at all for a container without a
healthcheck** — not "unhealthy", not "unknown", nothing. So a container-health
metric built today would be blank for 20 of 26 default-profile services.

Concretely: Plan 136's Stage 0b, as drafted, **would have caught the apiserver
incident and missed the solver incident** — and the two halves of that sentence
fail for *different* reasons, which is the whole argument of this plan.

It would have caught the apiserver because `airflow-apiserver` has a healthcheck
that correctly went red. It would have missed the solver **not** because `trawl`
lacks a healthcheck — it has one, `curl -sf localhost:8191/health` — but because
that healthcheck **returned `status:ok` for all eight hours** while the solve
rate sat at 0%, and the container showed `(healthy)` throughout
([Plan 136](plan_136_solver_recycle_and_liveness.md), Stage 4).

So the metric fails two ways at once, and Stage 1 only fixes the first:

- **Twenty services have no signal**, and a missing signal is invisible — a
  service with no healthcheck and a healthy service look identical. That is a
  coverage defect, and the `-1` state below is its fix.
- **`trawl` has a signal that lies.** No amount of coverage repairs that. It is
  an efficacy defect and it belongs to Plan 136 Stage 2.

Reading the first defect as the whole problem is the trap this plan is most
likely to fall into, because Stage 1 feels like completion.

### The signal already exists and is already collected

This is the part that makes the gap indefensible rather than merely unlucky.

`ops`, `scraper`, `processing` and `archiver` **all expose `/health`**
(`archiver/app.py:502`, `scraper/app.py:250`, `processing/app.py:24`,
`ops/app.py:83`), and `pack-worker` runs the archiver image. The Airflow DAGs
already poll every one of them through `http_health_sensor` — `ops`, `scraper`,
`processing`, `archiver`, `pack_worker`, `dbt_runner`.

So health is defined, exposed, and polled. **Its only consumer is a DAG sensor**,
which converts a health failure into a *downstream task failure*. That is
precisely how both incidents were detected: late, and named after the wrong
component. The 2026-08-18 page said `DAG scrape_listings failed`; the actual
fault was an apiserver connection pool.

Adding `healthcheck:` blocks to those five services is close to free. The
endpoint is already there. Nothing has wired it to Docker, and therefore nothing
has wired it to Prometheus.

## The three layers, and why none substitutes for another

Each incident so far failed at a different layer. This is the argument against
picking one and calling the job done.

| Layer | Question | Failed in |
|---|---|---|
| **Liveness** | Is the process up and answering? | Apiserver, 2026-08-18 — had a healthcheck; nothing watched it |
| **Freshness** | Is the signal itself current? | DuckDB gauges — stale values scraped as live ([Plan 136 D2](plan_136_solver_recycle_and_liveness.md)) |
| **Efficacy** | Is work actually succeeding? | `trawl`, 2026-08-14 — **healthy container, 0% solve rate, 8h outage** |

The solver incident is the reason liveness alone is insufficient: the container
was healthy the entire time and doing nothing useful. The apiserver incident is
the reason efficacy alone is insufficient: `statsd-exporter` reported normally
throughout while the service behind it was dead. A stale-proxy failure and a
healthy-but-useless failure are opposite shapes, and a monitoring design that
only has one of these layers will miss one of them.

## Goal

1. Every long-running service has a healthcheck, and **"no healthcheck
   configured" is a visible state rather than an absence.**
2. Container health is a Prometheus metric and an alert, for all services, not
   an allowlist someone maintains.
3. **A service added without health coverage fails CI**, not production.
4. DAG sensors stop being the de facto health signal.

## Design

### The principle: coverage is asserted, not enumerated

Every gap in this system's history has the same shape — *nobody added X to the
list*. `/mnt/data` was never added to node-exporter. `trawl` was never given a
healthcheck. Airflow was never added to `ct-service-down`. Each was fixed by
appending to a list, which sets up the next one.

The fix is not a longer list. It is **making an incomplete list fail loudly**,
at build time, on the machine of whoever shortened it.

**This repo already does something similar, in one place.**
`tests/test_observability_config.py` asserts Promtail and Prometheus job sets
with *exact set equality*:

```python
job_names = {job["job_name"] for job in doc["scrape_configs"]}
expected = {"ops", "scraper", "processing", "dbt_runner", "archiver", "pack-worker"}
assert expected == job_names, f"Unexpected promtail jobs: {job_names ^ expected}"
```

That detects drift inside a declared set, but it is not a universal service
coverage rule: not every service should be ingested into Loki. Plan 141 owns the
logging source-policy registry. This plan applies the same fail-loud principle
only to health coverage.

### Deny-list, never allowlist

An allowlist of "services we monitor" reproduces the defect: a service added
later is silently unwatched, which is the exact class of gap this plan exists to
close.

Instead: **every default-profile service is in scope by default**, and a short,
justified deny-list carries the genuinely transient ones:

| Service | Why exempt |
|---|---|
| `flyway` | Runs to completion and exits; `service_completed_successfully` is its contract |
| `airflow-init` | Same |
| profile-gated (`dbt`, `dbt_test`, `snapshot-worker`) | Not running under `docker compose up` |
| `oauth2-proxy` | Real unresolved hole: the current distroless image has no shell or HTTP client with which Docker can execute `/ping`; changing to the Alpine variant is a separate front-door image change |

`trawl` and `redis-trawl` are profile-gated but **long-running when up**, so they
are in scope. The 2026-08-14 outage is the reason to be explicit about that
rather than letting the profile flag decide.

Every deny-list entry needs a written reason in the test itself. A deny-list
that grows without justification is an allowlist wearing a disguise.

### "Unconfigured" must be loud

The metric needs three states, not two:

```
cartracker_container_health{container="ops"}     1   # healthy
cartracker_container_health{container="trawl"}   0   # unhealthy
cartracker_container_health{container="caddy"}  -1   # NO HEALTHCHECK CONFIGURED
```

Collapsing the third into "absent" is how a monitoring gap disguises itself as a
healthy system — the same mistake as Plan 136's D2, where a skipped refresh was
indistinguishable from a good reading. `-1` is ugly on a graph, which is the
point.

## Stages

### Stage 1 — Healthchecks everywhere — COMPLETE; SOAK VERIFIED 2026-08-20

Add `healthcheck:` to every in-scope service. Cheapest first, since the app tier
is nearly free:

- **`ops`, `scraper`, `processing`, `archiver`, `pack-worker`** — `/health`
  already exists; wire it. Match the existing Airflow blocks' shape
  (`interval: 30s`, `timeout: 10s`, `retries: 5`, `start_period: 30s`) so there
  is one convention rather than six.
- **`trawl`** — **nothing to add; it already has one.** That is precisely why it
  is the hardest case rather than the easiest: it was *healthy and useless* for
  eight hours, with `curl -sf localhost:8191/health` reporting `status:ok` the
  entire time. Its liveness check is present, correct, and **explicitly not
  sufficient**. Efficacy is Plan 136 Stage 2's job, and no edit in this stage
  should be read as covering it. Resist the urge to deepen this probe into a
  real solve — Plan 136 Stage 4 prices that at 30-90s per interval against
  cars.com and rejects it.
- **Infra** (`grafana`, `loki`, `prometheus`, `caddy`, `oauth2-proxy`,
  exporters) — well-known endpoints.

> The implementation corrected the draft's `curl --fail` default after checking
> the running images. The Python slim services have neither curl nor wget and
> use `urllib`; BusyBox-bearing images use `wget`; curl-bearing images use curl.
> Promtail has only bash and uses `/dev/tcp`. A healthcheck that fails because
> its probe tool is missing is worse than none — it manufactures a false
> unhealthy.

As built, all 25 probeable long-running services have an enabled healthcheck.
The one unresolved long-running service is `oauth2-proxy`: its distroless image
has no shell, curl, wget, or busybox, so Docker cannot express a probe against
its `/ping` endpoint. It remains explicit in the Stage 3 deny-list rather than
silently disappearing. Swapping the authenticated front door to
`latest-alpine` needs a separate deploy decision and verification.

**Verify:** `docker inspect --format '{{.State.Health.Status}}'` returns a real
status for every in-scope service, and no service flips unhealthy on a normal
cycle. Watch for false positives during `start_period` on the slow starters.

#### Production deployment and immediate verification — 2026-08-18

The operator declared deploy intent through the admin UI and waited for the
system to drain before recreating containers. The terminal transcript's earlier
`intent: none` reading preceded that admin action; it is not evidence that the
deploy-intent step was skipped. Intent was released only after the health and
smoke gates passed, after which the status returned to `intent: none` and normal
work resumed.

| Gate | Production evidence |
|---|---|
| Revision | `master` fast-forwarded from `8b2254b` to PR #216's merge commit `821a6a6` |
| Targeted apply | `docker compose --profile trawl up -d --no-deps ...` recreated all 18 intended services: 17 default-profile services plus the active `redis-trawl` profile service |
| Startup behavior | `loki` and `pgadmin` briefly reported `health: starting` inside their startup periods, then became healthy; no service became unhealthy |
| Runtime contract | All 25 services with configured checks reported `health=healthy` and `failing_streak=0`, including active `trawl` and `redis-trawl` |
| Expected no-health state | `flyway` and `airflow-init` were completed one-shots; running `oauth2-proxy` remained the documented distroless-image exception. Profile-gated one-shots `dbt`, `dbt_test`, and `snapshot-worker` were not created |
| Smoke checks | `http://localhost:8060/health` returned `{"ok":true}`; `https://cartracker.info/` succeeded; all four long-running Airflow services remained healthy |
| Compose warnings | Existing named volumes were reused and reported as not created by Compose; the warnings were non-blocking and no volume was replaced |

The immediate gate is therefore green. Keep Stage 1 open until the same audit
remains clean after 24 hours; only then begin Stage 2's metric and alert rollout.

#### 24-hour soak record — verified 2026-08-20

The audit was repeated at 2026-08-20 15:14 UTC, approximately 46 hours after the
17:03 UTC recreation and well past the 24-hour gate.

| Gate | Soak evidence |
|---|---|
| Runtime contract | All **25** services with configured checks reported `health=healthy` with `failing_streak=0`, including active `trawl` and `redis-trawl` |
| False positives | No service flipped unhealthy at any point in the window; the `loki` and `pgadmin` `health: starting` readings were confined to their original startup periods |
| Expected no-health state | Running `oauth2-proxy` remained the documented distroless exception; `flyway` and `airflow-init` remained completed one-shots |

Stage 1 is closed. The twenty new healthchecks did not produce a single false
page in 46 hours, which was the risk this soak existed to price.

#### What the soak found that Stage 2 must handle

The audit enumerated containers rather than compose services, and that surfaced
a defect Stage 2 would otherwise have shipped:

| Container | State | Compose project | In `docker-compose.yml`? |
|---|---|---|---|
| `cartracker-lakekeeper` | exited 0, reports `unhealthy` | `cartracker-lakehouse` | **No** |
| `cartracker-lakekeeper-postgres` | exited 0, reports `unhealthy` | `cartracker-lakehouse` | **No** |
| `cartracker-mlflow` | exited 137, reports `unhealthy` | `cartracker-mlflow` | **No** |
| `cartracker-lakekeeper-migrate` | exited 0, no health | `cartracker-lakehouse` | **No** |

**These are not orphans from deleted services.** They belong to two separate,
still-present compose projects — `docker-compose.lakehouse.yml` and
`docker-compose.mlflow.yml` — supporting
[Plan 125](plan_125_duckdb_to_iceberg_migration.md) and
[Plan 112](plan_112_refresh_policy_backtesting.md). All three long-running ones
stopped at the 2026-08-18 04:22 UTC host restart and did not come back, because
they carry no restart policy and are not part of the default project. Docker
still holds their last health state, and three report `unhealthy` permanently.

**A Stage 2 collector that walks `docker ps -a` would therefore publish four
permanent `0`s and page forever**, for services no one intends to be running.
This is the same failure shape the plan is built against, inverted: not a missing
signal, but a signal for something that should not be enumerated at all.

The distinction matters for how Stage 2 scopes itself. "Compose-managed" is not
a tight enough filter — these *are* compose-managed. The collector must scope to
**the running services of the default `cartracker` project**, and the `-1`
unconfigured state must apply only to services `docker-compose.yml` itself
declares. A sibling project that is deliberately down must be invisible to this
metric, not reported as broken.

That also sets the rule for the reverse case: if the lakehouse or MLflow stack
is ever brought up as part of normal production, it needs its own health
coverage decision rather than inheriting one by accident.

##### Resolved 2026-08-20 — containers removed, state preserved

Both sibling projects were taken down cleanly, which removed the four stale
health states at their source:

```bash
docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse down
docker compose -f docker-compose.mlflow.yml   -p cartracker-mlflow    down
```

**Neither command was given `-v`, and neither should be.** The projects' state
lives in named volumes rather than in the containers, and both survived intact:
`cartracker-lakehouse_lakekeeper_pgdata` at 67 MB — the Iceberg catalog Plan 125
Gate D depends on — and `cartracker-mlflow_mlflow_store` at 228 KB, holding Plan
112's tracking runs. `cartracker-net` is declared `external: true` in both files,
so Compose left the shared network alone; it still carries 26 containers.

Reversing this is `up -d` with the same `-f`/`-p` pair when Iceberg or
backtesting work resumes. Neither compose file has a top-level `name:` key, so
**the `-p` flag is required** — omitting it creates a differently-named project
that will not find the existing volumes.

The host now reports zero unhealthy containers, so Stage 2 can be built and
validated against a clean baseline. The scoping requirement above still stands
on its own: it must not be satisfied by this cleanup having happened, because
the next `up` of either project would reintroduce exactly the same condition.

### Stage 2 — The metric and the alert

Emit `cartracker_container_health` with the three states above, through the
node-exporter textfile collector [Plan 135](plan_135_storage_observability.md)
Stage 4 already built and proved. That is a second producer into working
plumbing rather than a new exporter.

Scope the collector to **the running services of the default `cartracker`
compose project**, for the reason the Stage 1 soak record documents above: four
containers from two deliberately-down sibling projects still carry a stale
`unhealthy` state, and enumerating `docker ps -a` would page on them
permanently.

Needs read-only Docker socket access. **Plan 136 Stage 4 proposes a
`docker-socket-proxy` for restart authority — if it has landed, read through it.
Do not add a second socket path.**

Alerts:

- `ct-container-unhealthy` — any `0` for 5m.
- `ct-container-health-unconfigured` — any `-1`. Not an incident; a **coverage**
  alert, routed accordingly. It should read as "this plan regressed."

Follow the both-directions validation Plan 131 used for
`ct-pack-verification-refused`: prove the selector matches live series, prove
the expression stays quiet on healthy data, then prove it fires — with a
deliberately stopped non-critical container, not by breaking something real.

### Stage 3 — CI asserts coverage — COMPLETE AND VERIFIED 2026-08-18

Extend `tests/test_observability_config.py`:

1. Every default-profile service not on the deny-list **has a healthcheck**.
2. The deny-list itself carries a reason string per entry.
3. Airflow's worst-case SQLAlchemy connection budget stays under Postgres
   `max_connections` (from [Plan 136](plan_136_solver_recycle_and_liveness.md)
   Stage 0a — it belongs with the other coverage invariants).

Logging coverage is deliberately absent from this list. Plan 141 defines its
different inclusion policy and tests it separately.

This is the stage that makes the rest durable. Without it Stages 1-2 are one
more enumeration with a longer list.

As built, `TestServiceHealthCoverage` rejects a missing, empty, or disabled
healthcheck; requires reasons for every live deny-list entry; keeps the two
long-running profile services in scope; rejects cross-container HTTP probes;
and prevents Python slim images from invoking an HTTP client they do not ship.
`TestAirflowConnectionBudget` remains the connection-budget invariant. The
existing CI unit-test command collects both classes through `pytest tests/`.

PR #216 passed Docker builds for all services, Ruff, the unit-test job, and the
dbt build-and-test job. The focused Stage 3 and connection-budget selection
passed all 10 tests, the full local unit suite passed 2,235 tests, and the
calculated Airflow worst-case remains 85 connections against Postgres's limit
of 100. Stage 3 requires no production soak and is closed.

### Stage 4 — Retire DAG sensors as the health signal

Once Stage 2 alerts exist, `http_health_sensor` should stop being how anyone
finds out a service is down. It stays useful as a **gate** — do not start
scraping if the scraper is unreachable — but its failure should no longer be the
first notification.

Judgment call, deliberately last: the sensors are load-bearing for DAG
correctness and should not be removed, only demoted. Verify by checking that a
deliberately stopped service pages via `ct-container-unhealthy` **before** the
next DAG run fails.

## Success criteria

1. Every in-scope service reports a real Docker health status.
2. `cartracker_container_health` covers all of them, with `-1` distinguishing
   unconfigured from healthy.
3. Adding a service to `docker-compose.yml` without a healthcheck **fails CI**.
4. A deliberately stopped non-critical container pages within 5 minutes, from
   the container-health alert and not from a downstream DAG failure.
5. The deny-list has a written reason for every entry.

## Risks

- **Twenty new healthchecks means twenty new ways to page falsely.** More likely
  to erode trust than any single missed alert. Stage 1 should land quietly and
  soak before Stage 2 alerts on it.
- **Healthcheck cost.** Twenty containers probing every 30s is negligible, but
  a probe that hits a DB or does real work is not. Keep them shallow — process
  liveness, never dependency health. A cascading healthcheck turns one Postgres
  blip into twenty unhealthy containers.
- **`-1` will be noisy at first**, by design, and there is a temptation to
  suppress it before Stage 1 finishes. That inverts the plan.
- **Liveness can be mistaken for sufficiency.** The single largest risk. The
  solver incident had a healthy container for eight hours. This plan makes the
  *floor* uniform; it does not make efficacy monitoring optional, and Plan 136
  Stage 2 remains necessary.

## Out of scope

- **Efficacy monitoring** — outcome counters and success-rate alerts are
  [Plan 136](plan_136_solver_recycle_and_liveness.md) Stage 2. This plan
  deliberately owns only the liveness floor and the coverage mechanism.
- **Automatic restart on unhealthy.** Plan 136 Stage 4 owns restart authority
  and the argument for who may call it. This plan produces the signal; it does
  not act on it.
- **Whole-host maintenance orchestration.** Plan 142 owns the separate
  maintenance state, drain, package/reboot procedure, and explicit resume. This
  plan supplies its mandatory health gate.
- **Analytics metrics freshness and serving ownership** —
  [Plan 143](plan_143_analytics_serving_snapshot.md).
- Replacing Docker healthchecks with an orchestrator's. That is Plan 88
  (Kubernetes), and it is not close.
