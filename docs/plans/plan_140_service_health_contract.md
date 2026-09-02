# Plan 140: The Service Health Contract — Coverage That Cannot Silently Rot

## Status

**COMPLETE (2026-08-25).** All four stages are deployed and verified in
production, and every success criterion is met.

Stages 1, 2 and 3 closed earlier; Stage 2's 24-hour soak closed clean on
2026-08-21 — see [the Stage 2 soak record](#24-hour-soak-record--closed-clean-2026-08-21).
**Stage 4 was deployed and verified 2026-08-25** — see
[the Stage 4 deploy record](#production-deployment-and-verification--2026-08-25)
and [the fire test](#the-fire-test--the-success-criterion-closed-2026-08-25).
**All four stages are now closed and every success criterion is met.**

Stage 4 came in as two slices rather than one keyword: demoting the sensors
would have left a stopped `archiver` or `pack-worker` with no notifier at all,
so Stage 4a closed the removed-or-stopped gap Stage 2 recorded before Stage 4b
flipped `soft_fail`. The fire test on 2026-08-25 stopped a container and
watched the metric publish `0` while the series count held at 28 — where before
the stage it would have published 27 and said nothing.
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

**Its cost rose on 2026-08-29.** This is no longer only a daily
`ct-container-health-unconfigured` page.
[Plan 142](plan_142_planned_host_maintenance.md) Stage 3's `container_health`
release gate fails on any service in `EXPECTED_SERVICES` not reading `1`, and
nothing in `ops/` or `container_health/` reads `healthcheck-exemptions.txt` — so
this one permanent `-1` blocks the resume gate of **every** host-maintenance
window, found while scoping the first one. Whichever plan resolves it first, the
two options are the same: teach the gate the documented exemption, or move the
front door to an image that can be probed. This plan owns the second.

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

#### As built — implemented 2026-08-20, deployed 2026-08-20, soak closed 2026-08-21

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

##### 24-hour soak record — closed clean 2026-08-21

Read at 16:35 UTC, **21h 28m** into the window that began with the `== bool`
correction at 2026-08-20 19:04 UTC. Called early by decision; the remaining
2h 30m is noted rather than waited out, and nothing below is trending toward a
change. Evidence is Grafana's own alert state-change history, not only a
reconstruction from Prometheus.

| Gate | Soak evidence |
|---|---|
| `ct-container-unhealthy` stays inactive | **Zero state-change annotations across the entire window.** Grafana recorded 51 alert transitions in 21h 28m and not one belongs to this rule. It reads `inactive`, 28 of 28 instances `Normal`, at the 16:33 evaluation |
| Zero false pages | **Zero.** The rule never reached `Pending`, let alone `Alerting` |
| Coverage stayed whole | 28 series at every one of 5,154 fifteen-second evaluations but one — see the limitation row below |
| The exporter itself never faltered | `min(up{job="container-health"})` = **1** over the window; no failed scrape |
| `oauth2-proxy` behaves as designed | `ct-container-health-unconfigured` went `Alerting` once at 19:08 and has held one instance since (`activeAt` 2026-08-20T19:08:00), the other 27 `Normal` |

**Three containers read `0`, and that is the result rather than a blemish on
it.** Each was a single 15-second sample:

| Container | When | Samples at `0` |
|---|---|---|
| `scraper` | 08-20 20:42:45 | 1 (~15s) |
| `processing` | 08-20 20:42:45 | 1 (~15s) |
| `airflow-scheduler` | 08-20 21:12:15 | 1 (~15s) |

The first two are the Plan 136 Stage 2 deploy recreating those containers. A
starting container reporting `0` for one scrape is the metric being *correct*,
and `for: 5m` is 20 samples wide — a 20× margin over the longest excursion the
window produced. This is the distinction the soak existed to prove: the metric
moves when Docker moves, and the alert does not.

The pre-correction `flaresolverr` and `grafana` `0` readings (08-20 18:38–18:47)
sit **before** the 19:04 baseline and are the original false-page incident
already recorded above, not soak findings.

**The known limitation reproduced live, exactly as written.** At 08-20 20:42:45
the series count read **27, not 28**, for one evaluation: `dbt_runner` left the
metric entirely while being recreated rather than reporting `0` (5,259 samples
against 5,260 for `scraper` and `processing`). That is the removed-or-stopped
gap documented in the section above, observed once, lasting 15 seconds, and
caused by a deploy. It cost nothing here and it is still real — closing it
remains Stage 4's expected-service set.

**Still owed:** nothing for this stage. Stage 4 is separate.

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

### Stage 4 — Retire DAG sensors as the health signal — DEPLOYED AND VERIFIED 2026-08-25

Once Stage 2 alerts exist, `http_health_sensor` should stop being how anyone
finds out a service is down. It stays useful as a **gate** — do not start
scraping if the scraper is unreachable — but its failure should no longer be the
first notification.

Judgment call, deliberately last: the sensors are load-bearing for DAG
correctness and should not be removed, only demoted. Verify by checking that a
deliberately stopped service pages via `ct-container-unhealthy` **before** the
next DAG run fails.

#### The stage is two slices, because the demotion alone would have created silence

The draft treats this as one keyword change, and priced it XS. It is not, and
the reason is a limitation this plan already recorded without noticing it had
become a blocker.

`ct-container-unhealthy` does not fire for a **stopped** container. Stage 2's
known-limitation section says so directly — the status filter in
`DockerApi.inspect_project_containers` admits only `running`, `restarting` and
`paused`, so a service that is gone leaves the metric rather than reading `0`.
Cross-referenced against `prometheus.yml`, that lands hardest on exactly the
services the sensors watch most:

| Service | Sensor call sites | `ct-service-down` | `ct-container-unhealthy` if **stopped** |
|---|---:|---|---|
| **`archiver`** | **7** | ✗ not a scrape job | ✗ series vanishes |
| **`pack-worker`** | **2** | ✗ not a scrape job | ✗ series vanishes |
| `scraper`, `processing`, `ops`, `dbt_runner` | 6 | ✓ | ✗ |

So for `archiver` and `pack-worker` — nine of the sixteen call sites — the DAG
failure *was* the only notification a stopped container produced. Flipping
`soft_fail` on its own would have traded a mis-named page for no page at all,
which is this plan's own failure mode committed by the stage meant to close it.

Hence 4a before 4b. Stage 2 anticipated this and said the fix "needs an
expected-service set… or Stage 4's DAG-sensor work."

#### Stage 4a — absence becomes a reading, not a gap

**The expected-service set already existed, in Plan 142.**
[`maintenance-running-set.txt`](../../maintenance-running-set.txt) (Plan 142
Stage 0 item 2, done 2026-08-23) records which services are expected running,
exceptions-only, one written reason per entry, checked against the Compose
sources by `tests/test_maintenance_running_set.py`. Plan 142 had also already
diagnosed this exact gap in the same words: *"A stopped container does not read
as unhealthy; it leaves the metric altogether… One absent service is therefore
silent by construction."*

The ownership boundary is what makes this a Plan 140 change against a Plan 142
input, rather than either plan reaching into the other:

- Plan 142 owns the manifest, and owns the auxiliary-project check outright —
  its Finding 2 is explicit that Plan 140's project-scoped metric *cannot*
  answer "are the paused sibling projects still paused."
- Plan 140 owns making absence visible in the metric. Plan 142 Stage 3's resume
  gate then consumes it, requiring "neither unhealthy nor unconfigured services
  hidden as absence."

**A first draft restated the rule instead of consuming the manifest, and was
wrong.** "Expected running == declares a `restart:` policy other than `no`"
looks equivalent, reproduces the same 28 names today, and silently drops the
`restart-gap` class — a service that *is* expected running and merely does not
restore itself after a reboot. `caddy` was precisely that until 2026-08-24, and
it serves `:80` and `:443`. A derivation that is accidentally right today and
structurally wrong is worse than the list it replaces.

| Decision | As built |
|---|---|
| Source of truth | `expected_running_services()`, extracted from the existing inline derivation in `tests/test_maintenance_running_set.py`. No second rule, no second list |
| Delivery | Resolved at build time into `container_health/expected.py`. The exporter image still copies only its own package and reads no repo file at runtime — and the manifest could not be read alone anyway, since resolving it needs `docker-compose.yml` |
| Drift guard | `TestExpectedServicesMatchTheManifest` asserts exact set equality. A service added to Compose, or reclassified in the manifest, fails CI rather than going unwatched |
| Value for an absent service | `0`. Not a fourth state — Stage 2's argument that a fourth value re-opens the ambiguity `-1` exists to close still holds, and `0` already means "should be healthy and is not" |
| Ordering | The backfill runs **after** the `NoContainersFound` guard. Reversed, a project-label mismatch would publish `0` for all 28 services and page for the whole fleet, burying the one fact that matters |
| Unexpected-but-running | Still published at its real value. Hiding a service someone started by hand is the same disappearing-signal defect in a new place |

The set is 28 services, matching Stage 2's production reading exactly — 28
services, 27 at `1`, one `-1`. `oauth2-proxy` is deliberately in it: the
healthcheck deny-list answers "can this be probed", which is a different
question from "should this be up", and conflating them would have dropped the
one service already known to be a coverage hole.

#### Stage 4b — the demotion

`http_health_sensor` gains `soft_fail=True`. On timeout it now raises
`AirflowSkipException` instead of `AirflowSensorTimeout`, so downstream
`all_success` tasks skip, the run ends successfully having done nothing, and
`airflow_dagrun_duration_failed_count` never increments — which is what
`ct-pipeline-failures` selects on. The gate is untouched: no work runs against a
service that is not answering.

Verified against the real Airflow 3.2.0 source rather than assumed, because the
parameter's behaviour has moved between versions. In `task-sdk`
`airflow/sdk/bases/sensor.py`, the timeout branch checks `self.soft_fail` and
raises `AirflowSkipException` **before** the `if self.reschedule` branch, and
`run_duration()` accumulates from `ti.get_first_reschedule_date()` rather than
resetting per poke. [Issue #61130](https://github.com/apache/airflow/issues/61130)
— deferrable sensors ignoring `soft_fail` on timeout — does not apply, because
these are `mode="reschedule"`. `test_the_sensor_is_not_deferrable` pins that:
switching modes would silently restore the failure while leaving `soft_fail=True`
in place to suggest otherwise.

**Two DAGs wired a health sensor directly into a Telegram notifier**, which is
the literal form of the defect this stage names rather than an emergent one:

- `hourly_analytics_refresh.py` — `[ready, archiver_up, …, dbt_runner_up, …] >> notify`
- `pack_bronze_html.py` — `[ready, pack_worker_up, …] >> notify`

with `trigger_rule="one_failed"`. An unreachable `archiver` sent "hourly
analytics refresh FAILED" — naming the DAG, not the service. The sensors are
removed from both fan-ins. `soft_fail` alone would have quieted them, since a
skipped upstream does not satisfy `one_failed`, but leaving the wiring in place
means the next `trigger_rule` edit silently re-arms it.

`deploy_intent_sensor` is deliberately **not** demoted. A stuck deploy intent is
a different condition with a different owner — Plan 142 Stage 1 item 3 replaces
its 600-second failure with a maintenance-aware gate — and skipping it here
would let work start mid-deploy.

`ct-container-unhealthy`'s description was rewritten for the case 4a adds. It
previously sent the operator straight to `docker inspect … State.Health.Log`,
which returns nothing useful for a container that is gone; it now separates the
two readings and names `docker ps -a` first.

#### Production deployment and verification — 2026-08-25

Deployed to `73f9d4d` through `scripts/redeploy.sh`, the Plan 144 path, rather
than by hand. Deploy intent was declared at 02:09:29 UTC and drained
immediately (`number_running: 0`); the script released it on success.

Only `container-health` was rebuilt and recreated. The Airflow half needed no
container action at all: `./airflow/dags` is a **directory** bind mount, so the
DAG changes landed with the `git pull` and the dag-processor re-serialized on
its own. `grafana/provisioning` is likewise a directory mount, immune to the
inode trap Stage 2 recorded, but Grafana reads alerting provisioning at startup
and so was restarted. `prometheus.yml` was unchanged.

The rebuild was mandatory rather than hygienic: `container_health/expected.py`
is a new file, `app.py` imports it, and a cached image would have crash-looped
on `ImportError`. The build log confirms the `COPY container_health/` layer was
not cached.

| Gate | Evidence |
|---|---|
| Exporter loaded the new code | `EXPECTED_SERVICES` imported inside the running container: **28** |
| Metric unchanged by the change | **28 series**, 27 at `1`, `oauth2-proxy` at `-1` — identical to the pre-deploy reading, so 4a added no series and flipped nothing |
| Prometheus ingesting | 28 |
| DAG re-serialization | 0 import errors; **16 of 16** health sensors carry `soft_fail=True` |
| The deliberate exception held | `check_deploy_intent` does **not** carry it |
| Post-deploy runs | `orphan_checker` and `results_processing` both succeeded; zero failed tasks; zero skipped sensors |
| Health gate | `container-health` healthy in 5s, `grafana` in 6s |

##### The fire test — the success criterion, closed 2026-08-25

Stage 2's validation used `docker pause`, which exercised only the `paused → 0`
path; Stage 2 recorded explicitly that a *stopped* container would have left
the metric instead. 4a is what makes the real test possible, so it was run.

| Event | UTC |
|---|---|
| `docker stop cartracker-flaresolverr` | 02:32:36 |
| Docker reports `exited`; **metric reads `0`, series count holds at 28** | 02:32:41 |
| Prometheus ingests the `0` | 02:32:45 |
| `ct-container-unhealthy` → **Alerting**, `container=flaresolverr` | **02:38:00** |
| `docker start`, healthy, back to `1.0`, 28 series | 02:42:22 |

**The series count holding at 28 is the whole result.** Before this stage the
exporter would have published 27 and the stopped service would have been
invisible — no `0`, no alert, nothing. The 5m24s to Alerting is the rule's 5m
`for` plus scrape and evaluation latency.

`ct-container-health-unconfigured` stayed on `oauth2-proxy` throughout, with
`activeAt` still 2026-08-20 19:08, so the new firing did not disturb the
standing coverage alert.

One honest limit on how far this proves the criterion's second half. The
criterion is "pages **before** the next DAG run fails", and `flaresolverr` is
vestigial — no DAG senses it — so no DAG run was ever going to fail here. What
was observed is the alert arriving on its own, from the health signal, with no
pipeline failure anywhere. For a *sensed* service the comparison is now
structural rather than empirical: a health sensor times out at 600s and, after
Stage 4b, **skips** rather than fails, so the DAG path can no longer produce a
page at all. The alert at ~5m is the only notifier left, which is the outcome
this stage was for.

##### Interaction with the two open soaks — checked, not assumed

The deploy landed between Plan 142's Phase A gate and Plan 136's Stage 3a read,
so both were checked rather than hoped over.

- **Plan 136 Stage 3a is intact.** `cartracker-trawl` still reports
  `StartedAt 2026-08-22T17:52:56Z` — it was never touched, so the memory curve
  is continuous and the 19:40 UTC baseline read is unaffected. Recreating
  `container-health` cost a few 15-second samples of
  `cartracker_container_memory_bytes`; the series was publishing again
  immediately (`trawl` at 2.67 GB against its 4 GB cap). The exporter restart
  does not reset the curve, because the curve is `trawl`'s memory, not the
  exporter's.
- **Plan 142's items 6 and 7 landed inertly.** The `git pull` brought 22
  commits including `8045e7e`, whose `caddy` restart policy and Airflow JWT
  secret are *service config* — they take effect only on `up -d` for those
  services, and `redeploy.sh` names one service and passes `--no-deps`. Neither
  `caddy` nor the four Airflow services was recreated, so the public site never
  dropped and no JWT rotated. Those changes now sit on disk awaiting Phase B's
  window, which is where they were always meant to be applied.

##### Still owed

Nothing for this stage. Plan 140's success criteria are all met; see the
criteria list below.

## Success criteria — all met as of 2026-08-25

1. ✅ Every in-scope service reports a real Docker health status. 25 of 26
   probeable long-running services at Stage 1; `oauth2-proxy` remains the
   documented distroless exception, visible as `-1` rather than as absence.
2. ✅ `cartracker_container_health` covers all of them, with `-1` distinguishing
   unconfigured from healthy — and, since Stage 4a, `0` distinguishing
   *stopped* from absent.
3. ✅ Adding a service to `docker-compose.yml` without a healthcheck **fails
   CI** (`TestServiceHealthCoverage`).
4. ✅ A deliberately stopped non-critical container pages within 5 minutes,
   from the container-health alert and not from a downstream DAG failure.
   Closed by [the 2026-08-25 fire test](#the-fire-test--the-success-criterion-closed-2026-08-25):
   `flaresolverr` stopped 02:32:36, `ct-container-unhealthy` Alerting 02:38:00,
   no DAG failure involved. Note the limit recorded there — `flaresolverr` is
   sensed by no DAG, so the "not from a DAG failure" half is now structural
   rather than empirical: after Stage 4b a health sensor skips instead of
   failing, so that path cannot page at all.
5. ✅ The deny-list has a written reason for every entry
   (`healthcheck-exemptions.txt`, asserted by
   `test_every_deny_list_entry_carries_a_reason`).

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
