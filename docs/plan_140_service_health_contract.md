# Plan 140: The Service Health Contract — Coverage That Cannot Silently Rot

## Status

**STAGES 1 AND 3 COMPLETE AND VERIFIED. STAGE 2 IS IMPLEMENTED AND VALIDATED
AGAINST A REAL DOCKER DAEMON; ITS PRODUCTION DEPLOY AND SOAK ARE OWED** — see
"As built" under Stage 2.
[PR #216](https://github.com/whitewalls86/new_car_tracker/pull/216) merged the
implementation as `821a6a6`, adding eighteen healthchecks and taking configured
coverage from 7 of 31 services to 25 of 31. The six remaining services are the
five deliberate one-shot/profile exemptions plus `oauth2-proxy`, whose current
distroless image cannot execute a probe. The immediate production gate passed:
all 25 configured runtime checks were healthy with zero failing streaks. **The
24-hour soak closed clean on 2026-08-20** — see the soak record below. Stage 3's
fail-loud CI contract is complete and verified. Stage 2's metric and alerts are
built, and they handle the scoping hazard that soak surfaced.

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

Emit `cartracker_container_health` with the three states above.

#### Amended 2026-08-20: a dedicated exporter, not the textfile collector

This stage originally specified node-exporter's textfile collector, on the
reasoning that [Plan 135](plan_135_storage_observability.md) Stage 4 had already
built and proved that plumbing, so this would be "a second producer into working
plumbing rather than a new exporter."

**That reasoning does not survive comparing the two producers' cost profiles.**
Plan 135 needs the textfile collector because a `du -s -x` walk of the watchlist
took 456 seconds — work that cannot happen inside a scrape handler, so it must
be done ahead of time and left in a file. Reading container health is a single
Docker API call in the low tens of milliseconds. It is the opposite case: cheap
enough to generate *at scrape time*.

Generating at scrape time removes work rather than adding it:

| Concern | Textfile collector | Dedicated `/metrics` |
|---|---|---|
| Staleness | Needs a companion timestamp metric, a staleness alert, and carry-forward reasoning | **Structurally impossible** — the value is computed when Prometheus asks |
| Liveness of the collector itself | Nothing watches it; a dead writer reads as a healthy fleet | `up{job="container-health"}` |
| Socket grant lands on | A service that also holds Postgres and MinIO credentials | A container with no other credentials |
| Shared volume and atomic writes | Required | Not needed |

This follows [Plan 143](plan_143_analytics_serving_snapshot.md)'s pattern — the
service that owns the data exposes `/metrics` and Prometheus scrapes it directly
— rather than Plan 135's. Add a `container-health` scrape job to
`prometheus/prometheus.yml`.

The staleness problem this deletes is not hypothetical. It is precisely what
Plan 143 spent a full 24-hour soak correcting, and Plan 136 D2 before it.

#### Scope: the default project's running services

Scope the collector to **the running services of the default `cartracker`
compose project**, for the reason the Stage 1 soak record documents above.

Note that "compose-managed" is *not* a sufficient filter — the four containers
that soak found were compose-managed, just by a different project. Key on the
`com.docker.compose.project` label.

#### Socket access: `docker-socket-proxy`, and why `:ro` is not enough

**Do not mount `/var/run/docker.sock` with `:ro` and call it read-only.** That
flag makes the socket *file* read-only; it does nothing to the Docker API
reachable through it. Any client that can connect can issue
`POST /containers/{id}/restart`, `kill`, or create a privileged container.

`docker-socket-proxy` with `CONTAINERS=1` and `POST=0` is the only option that
actually enforces read-only access, and it gives
[Plan 136](plan_136_solver_recycle_and_liveness.md) Stage 4 a narrow, explicit
place to later grant exactly one verb. **Do not add a second socket path** when
that stage lands.

`promtail` already carries a `/var/run/docker.sock:...:ro` mount
(`docker-compose.yml:951`) for log-discovery metadata, which is the same
full-API grant. That is pre-existing and out of scope for this stage, but it
should not be read as a precedent that settles this decision.

#### Alerts

- `ct-container-unhealthy` — any `0` for 5m.
- `ct-container-health-unconfigured` — any `-1`. Not an incident; a **coverage**
  alert, routed accordingly. It should read as "this plan regressed."

No staleness alert is needed, because the exporter cannot serve a stale value.
Its liveness is `up{job="container-health"}`.

**That last point requires fixing `ct-service-down` first.** It currently selects
`up{job=~"ops|processing"}` — an allowlist covering two of eight scrape jobs,
which is the same defect this plan exists to close, sitting in the alert file.
Until it covers every job, "the exporter's liveness is `up`" is an aspiration
rather than a fact. Widen it, and add a coverage test in the same style as
`TestServiceHealthCoverage`.

Follow the both-directions validation Plan 131 used for
`ct-pack-verification-refused`: prove the selector matches live series, prove
the expression stays quiet on healthy data, then prove it fires — with a
deliberately stopped non-critical container, not by breaking something real.

#### As built — implemented 2026-08-20, production deploy pending

`container_health/` is a dedicated service of four small modules:
`collector.py` holds the pure state mapping and the scoping rule,
`docker_api.py` holds the read client, and `app.py` serves `/health` and
`/metrics`. It imports nothing from `shared/`, mounts no volume, and its
`Dockerfile` copies only its own package rather than the repo — the container
holding the Docker grant should carry as little else as possible.

| Decision | As built |
|---|---|
| Mechanism | `prometheus_client` custom collector; the Docker read happens inside the `/metrics` handler. No file, no timestamp metric, no staleness alert |
| Socket | `tecnativa/docker-socket-proxy:0.3.0` with `CONTAINERS=1`, `POST=0`, on a two-member `internal: true` network the other ~26 containers cannot reach |
| Scope | `com.docker.compose.project=cartracker`, applied server-side as an optimisation and re-applied in the unit-tested collector as the authoritative rule |
| Credentials | `DOCKER_API_URL` and `COMPOSE_PROJECT`, and a test asserting that is the whole environment |
| Port | 9110, scraped as job `container-health` |

Four details the draft did not anticipate:

- **`starting` is a fourth Docker state and it maps to `0`.** "Not yet known
  to be healthy" is not healthy, and a fourth metric value would re-open the
  ambiguity `-1` exists to close. It is safe because the state is *bounded* —
  Docker leaves it within `start_period + retries × (interval + timeout)`,
  a 230s worst case across this compose file, against the alert's 300s `for`.
  That is not a comment but an assertion:
  `test_unhealthy_alert_outlasts_the_slowest_healthcheck_start` recomputes the
  worst case from the compose file, so widening a `start_period` past the
  alert's `for` fails CI instead of paging on the next deploy.
- **`restarting` and `paused` are enumerated, and map to `0`.** The draft's
  "running services" would have made a crash-looping container *vanish* from
  the metric, which is the same disappearing-signal defect in a new place.
- **`docker compose run` one-shots carry the project label.** A running
  `dbt`, `dbt_test`, or `snapshot-worker` invocation would otherwise publish
  `-1` and page the coverage alert for its duration. Excluded by
  `com.docker.compose.oneoff`.
- **An empty result refuses rather than publishes.** This exporter is itself a
  member of the fleet, so zero matching containers means the project label
  stopped matching — a renamed deploy directory, a `COMPOSE_PROJECT_NAME`
  change. Publishing nothing would read as a healthy system. It raises, so
  `/metrics` returns 500 and `up{job="container-health"}` goes to 0.

**`ct-service-down` was widened from two jobs to all nine** and given
`test_service_down_covers_every_scrape_job`, which asserts its job set equals
`prometheus.yml`'s by exact set equality. `airflow`, `postgres`, `minio`,
`minio_bucket`, `dbt_runner` and `node` had no target-level alert at all.

`ct-container-health-unconfigured` carries `severity: coverage` and a nested
notification route repeating daily instead of the 4h incident cadence. Same
receiver — it is visible, not suppressed — but a missing healthcheck needs a
compose edit, not a 3 a.m. response, and training an operator to ignore the
scraper's alert channel is how the 2026-08-14 outage stayed invisible.

##### The dashboard, which the draft omitted

The stage as drafted produced a metric and two alerts and nothing that renders
either. That is a gap, not a detail: an alert-only signal is one nobody looks
at until it pages, and it gives an operator responding to
`ct-container-unhealthy` nowhere to land.

A **Service Health** row now opens the Infrastructure dashboard — *above* the
storage panels, because "is the fleet up?" is the first question it should
answer, not the ninth. Three stat tiles (healthy / unhealthy / no healthcheck)
and a `state-timeline` keyed on `{{container}}`, with `-1` mapped to an orange
"no healthcheck" band rather than to a gap in the chart. The tiles use
`or vector(0)`, since `count()` over an empty match returns no series and
Grafana draws that as "No data" — at a glance indistinguishable from a broken
exporter, which is this plan's own failure mode reappearing in the UI.

Verified by provisioning the real `grafana/grafana:11.6.1` image against this
repo's provisioning directory: 23 panels loaded, `state-timeline` accepted, the
value mappings survived the round trip, all 18 alert rules provisioned, and the
`severity=coverage` route resolved to a `1d` repeat beside the parent's `4h`.

##### Production deployment and the defect it exposed — 2026-08-20

Deployed to `0a9fba9`. `docker compose up -d --no-deps docker-socket-proxy
container-health` created only the two new containers and one network — a
`--dry-run` confirmed nothing existing would be recreated, which is why this
deploy did **not** declare deploy intent: nothing in the scrape, processing or
dbt path was touched, and draining 597 in-flight artifacts would have bought
nothing.

The metric came up correct on the first read: 28 services, 27 at `1`, and
exactly one `-1` — `oauth2-proxy`, the documented distroless exception. All
nine scrape targets `up`. `flyway` and `airflow-init` correctly absent as
exited one-shots, and no sibling-project container appeared.

**Two things went wrong, and both are worth more than the deploy itself.**

**1. A single-file bind mount is pinned to an inode, so `git pull` orphans it.**
`prometheus.yml` is mounted as a *file*, not a directory. `git pull` replaces
the file rather than editing in place, so the new content landed on a new inode
while the container kept reading the old one. `docker kill -s HUP` reloaded
happily and logged *"Completed loading of configuration file"* — against the
pre-pull config, with no `container-health` job in it. The reload was truthful
about what it did and silent about what it read.

```bash
stat -c %i prometheus/prometheus.yml                                   # 519700
docker exec cartracker-prometheus stat -c %i /etc/prometheus/prometheus.yml  # 519794
```

`docker restart` fixes it — Docker re-resolves bind mounts at container start —
and a recreate is not needed. This affects every single-file mount delivered by
git: `prometheus.yml`, `promtail.yml`, `loki.yml`, `statsd_mapping.yml`.
Directory mounts (`grafana/provisioning`, `grafana/dashboards`) are immune.
**This belongs in [Plan 144](plan_144_deploy_script_hardening.md)**, whose whole
subject is a deploy script that cannot tell you whether it worked.

**2. The alert expression manufactured a false page in six minutes.** The rules
shipped as `count by (container) (cartracker_container_health == 0)`. A
*filtering* comparison drops the series when a container is healthy, and
Grafana's `reduce: last` over the 600s `relativeTimeRange` then keeps the last
value it saw alive for the rest of that window.

Restarting Grafana to load its own provisioning put `container="grafana"` at `0`
for **one 15-second sample** at 18:38:15 — correct behaviour, that is the
`starting` state working as designed. The ghost series then satisfied the 5m
`for`, and at 18:44 the rule was Alerting for a container healthy since 18:38:30
and reporting `1` in Prometheus the whole time.

Uncorrected, that pages on **every deploy, for every container restarted** —
precisely the "twenty new ways to page falsely" this plan lists as its top risk,
and the same stale-value-read-as-live shape as Plan 136 D2. The fix is
`== bool 0`, which returns 1-or-0 for every series and drops none, so a
recovered container reads `0` on the next evaluation.

It is worth being precise about why the other rules in this file do not share
the bug: they select *continuous* gauges that always carry a current sample,
where `last` genuinely is the latest value. Only an expression whose series come
and go is exposed, which is why the guard belongs on the expression rather than
on the shared `reduce`/`relativeTimeRange` shape.

| Validation | Result |
|---|---|
| Selector matches live series | 28 |
| Stays quiet on healthy data | `ct-container-unhealthy` matched nothing |
| Coverage alert fires correctly | `ct-container-health-unconfigured` → `oauth2-proxy`, routed on the `1d` coverage cadence |
| `ct-service-down` covers all nine | All nine jobs matched, none down |
| Fires on a real fault | `docker pause cartracker-flaresolverr` (vestigial; the live path is `trawl`) → metric `0` within 25s, Prometheus ingested it, alert Pending at 18:44:00. Unpaused 18:46:30 |

The pause also exercised the `paused → 0` path this stage added beyond the
draft, and confirmed the known limitation below: a *stopped* container would
have left the metric instead of reporting `0`.

##### Corrected and re-verified — 2026-08-20 19:04 UTC

The `== bool` fix deployed at `0d07ed9`. Note that `grafana/provisioning` is a
*directory* mount, so it was immune to the inode trap above — the container
already saw the new file; the restart was needed only because Grafana reads
alerting provisioning at startup.

| Rule | State after the fix |
|---|---|
| `ct-container-unhealthy` | **inactive**, with all 28 containers explicitly `Normal` |
| `ct-container-health-unconfigured` | `oauth2-proxy` `Pending`, the other 27 `Normal` |

`flaresolverr` cleared the moment the rule reloaded, having been healthy since
18:46:30 while the old expression held it firing for eleven minutes past
recovery.

The corrected form is a better signal than the original intent, not merely a
bug fix: every container now carries an explicit per-evaluation state instead of
appearing only while broken. "All 28 evaluated, one pending" is readable
directly, where the filtering form required inferring health from absence —
which is the same inference this entire plan exists to stop making.

**`oauth2-proxy` will now page once per day** on the coverage cadence until it
has a healthcheck. That is the designed behaviour, not a defect to tune away:
it is a real, unresolved coverage hole, and the plan's stated risk is the
temptation to suppress `-1` rather than close it. Closing it means swapping the
front door to `latest-alpine`, which remains a separate deploy decision.

##### Known limitation, recorded rather than discovered later

A service whose container is **removed or fully stopped** leaves the metric
entirely rather than reporting `0`. `restart: unless-stopped` means the
realistic failure is a crash loop, which is covered above as `restarting`, and
the six scraped services are covered by the widened `ct-service-down`. But
`caddy`, `grafana`, `loki` and `promtail` going away cleanly would be silent
here. Closing that needs an expected-service set, which means either parsing
`docker-compose.yml` at runtime — profiles, deny-list and all, duplicated in
two places — or Stage 4's DAG-sensor work. It is deliberately not in this
stage.

##### Local validation, 2026-08-20 — against a real daemon, before production

Run against a purpose-built fixture fleet on a Docker 29.1.3 host rather than
mocked, because the whole metric is a claim about what Docker actually reports:

| Check | Result |
|---|---|
| Three states render together | `healthy-one` 1, `unhealthy-one` 0, `no-check` -1 |
| Sibling projects invisible | Four separate live and stopped projects — including three `Exited` containers with exactly the stale-`unhealthy` shape the Stage 1 soak found — produced no series |
| `POST` is genuinely refused | `POST /containers/{id}/restart` through the proxy returned **403 Forbidden** |
| Probe tools exist in the images | `wget /_ping` in the Alpine-based proxy and `python -c urllib` in the exporter both exited 0 — the false-unhealthy trap Stage 1 recorded |
| Empty fleet refuses | A deliberately wrong `COMPOSE_PROJECT` returned **500**, not an empty page of metrics |
| Exporter liveness is real | With the proxy stopped, `/metrics` 500'd while `/health` still returned `{"ok":true}` — the shallow probe stays shallow and `up` is what carries the signal |
| Scrape cost | ~20ms for the fixture fleet; the production Docker read measured **~120ms for 26 containers**, list plus per-container inspect |

That last figure corrects this plan's "low tens of milliseconds" estimate by an
order of magnitude, and does not change the conclusion: 120ms inside a 15s
scrape is 0.8% duty, against the 456 seconds that forced Plan 135 to use a
file. The per-container inspect is deliberate — `Health` only appears in the
list endpoint at API v1.52 (Docker 29), and measurement showed the single
richer call costs the same ~120ms, so version-robustness here is free.

**Still owed:** production deploy, the fires-and-stays-quiet halves of the
validation above against live series, and the soak.

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
