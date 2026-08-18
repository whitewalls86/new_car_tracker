# Plan 140: The Service Health Contract — Coverage That Cannot Silently Rot

## Status

DRAFT, written 2026-08-18 after the second incident in four days where **no
alert caught the failing component** and a human found it by noticing damage
downstream. Nothing applied.

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
| Profile-gated | 5 | `trawl`, `redis-trawl`, `dbt`, `dbt_test`, `snapshot-worker` |

**Docker reports no health status at all for a container without a
healthcheck** — not "unhealthy", not "unknown", nothing. So a container-health
metric built today would be blank for 20 of 26 services.

Concretely: Plan 136's Stage 0b, as drafted, **would have caught the apiserver
incident and missed the solver incident.** `airflow-apiserver` has a healthcheck;
`trawl` does not. That asymmetry is invisible in the metric — a service with no
healthcheck and a service that is fine look identical.

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

**This repo already does exactly that, in one place.**
`tests/test_observability_config.py` asserts promtail and Prometheus job
coverage with *exact set equality*:

```python
job_names = {job["job_name"] for job in doc["scrape_configs"]}
expected = {"ops", "scraper", "processing", "dbt_runner", "archiver", "pack-worker"}
assert expected == job_names, f"Unexpected promtail jobs: {job_names ^ expected}"
```

Add a service without a log job and this test fails. Add a log job without
updating the test and it also fails. That is the correct shape and it already
works — it is simply applied to one concern out of four.

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

### Stage 1 — Healthchecks everywhere

Add `healthcheck:` to every in-scope service. Cheapest first, since the app tier
is nearly free:

- **`ops`, `scraper`, `processing`, `archiver`, `pack-worker`** — `/health`
  already exists; wire it. Match the existing Airflow blocks' shape
  (`interval: 30s`, `timeout: 10s`, `retries: 5`, `start_period: 30s`) so there
  is one convention rather than six.
- **`trawl`** — the 2026-08-14 outage's service, and the hardest case: it was
  *healthy and useless*. A liveness check here is necessary and **explicitly not
  sufficient**; efficacy is Plan 136 Stage 2's job and this stage must not be
  read as covering it.
- **Infra** (`grafana`, `loki`, `prometheus`, `caddy`, `oauth2-proxy`,
  exporters) — well-known endpoints.

> Use `curl --fail` consistently, and **verify the image actually has `curl`**.
> Plan 135 recorded BusyBox `wget` inside the Prometheus container reporting 503
> where host `curl` reported 200. A healthcheck that fails because the tool is
> missing is worse than none — it manufactures a false unhealthy.

**Verify:** `docker inspect --format '{{.State.Health.Status}}'` returns a real
status for every in-scope service, and no service flips unhealthy on a normal
cycle. Watch for false positives during `start_period` on the slow starters.

### Stage 2 — The metric and the alert

Emit `cartracker_container_health` with the three states above, through the
node-exporter textfile collector [Plan 135](plan_135_storage_observability.md)
Stage 4 already built and proved. That is a second producer into working
plumbing rather than a new exporter.

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

### Stage 3 — CI asserts coverage

Extend `tests/test_observability_config.py`:

1. Every default-profile service not on the deny-list **has a healthcheck**.
2. Every such service **appears in the promtail job set** (generalizing the
   existing exact-set test beyond the six app services).
3. The deny-list itself carries a reason string per entry.
4. Airflow's worst-case SQLAlchemy connection budget stays under Postgres
   `max_connections` (from [Plan 136](plan_136_solver_recycle_and_liveness.md)
   Stage 0a — it belongs with the other coverage invariants).

This is the stage that makes the rest durable. Without it Stages 1-2 are one
more enumeration with a longer list.

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
3. Adding a service to `docker-compose.yml` without a healthcheck or a log job
   **fails CI**.
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
- **Metrics freshness** — Plan 136 Stage 1.
- Replacing Docker healthchecks with an orchestrator's. That is Plan 88
  (Kubernetes), and it is not close.
