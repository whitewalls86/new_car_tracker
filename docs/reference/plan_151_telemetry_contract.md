# Plan 151 Stage A — Telemetry contract, resource budget, and experiment design

Stage A of [Plan 151](../plans/plan_151_distributed_tracing_and_runtime_topology_audit.md)
deploys nothing. Its exit is this document: the eight stage items answered for
the selected paths, the resource budget with abort thresholds recorded before
deployment, and the Stage A gate decided.

> **Decision, 2026-09-08: do not build the telemetry pipeline.** Stage A found
> that the audit signal Plan 151 was designed to produce is obtainable by static
> analysis of checked-in files, at a small fraction of the cost — 62 undeclared
> edges from ~80 lines of derivation with no runtime component (§5.1). Stages B
> through E do not run. This is the plan's own **Stage E "remove"** outcome,
> reached four stages early and on evidence; the plan states that a decision of
> remove "is a successful exit, not a failed one." §10 records the reasoning and
> what would justify revisiting it.
>
> Everything below §1–§8 is the cost side of that decision and is retained
> deliberately: the conclusion is only defensible because the pipeline was
> costed properly before it was declined. §1 also contains two defects
> (**F1**, **F3**) that outlive this plan.

All measurements were taken from the production VM on **2026-09-08 20:14 UTC**
(uptime 8d 2h). Every number below is reproducible by the commands in
[§9](#9-how-these-numbers-were-taken); none is estimated unless the text says so.

---

## 1. Baseline inventory

### Host

| Property | Measured |
|---|---|
| Platform | OCI A1.Flex, `aarch64` |
| CPU | 4 vCPU, load average 0.69 / 1.01 / 1.25 |
| Memory | 23.4 GiB total, 5.6 GiB used, **17.4 GiB available**, no swap |
| `/` (`/dev/sda1`) | 49 GB, 45% used, **27 GB available** |
| `/mnt/data` (`/dev/sdb`) | 196 GB, 21% used, 147 GB available |

Container memory limits are set on exactly three services today —
`dbt_runner` (12 GiB), `trawl` (4 GiB), `redis-trawl` (512 MiB). Everything else
is unbounded against the host's 23.4 GiB.

### Existing telemetry stack

| Property | Measured |
|---|---|
| Prometheus active head series | **7,347** (6,895 attributable to a job) |
| Prometheus chunks | 34,343 |
| Prometheus TSDB on disk | **1.4 GB** at 30d retention |
| Prometheus RSS | 188.7 MiB |
| Scrape interval | 15s |
| Active scrape targets | 11, all `up` |
| Loki on disk | 3.5 GB |

Series by job:

| Job | Series | | Job | Series |
|---|---:|---|---|---:|
| postgres | 3,350 | | minio_bucket | 144 |
| node | 1,214 | | processing | 106 |
| airflow | 1,104 | | container-health | 39 |
| promtail | 296 | | dbt_runner | 20 |
| ops | 276 | | **TOTAL** | **6,895** |
| minio | 198 | | | |
| scraper | 148 | | | |

### Derived marginal cost of a Prometheus series

7,347 series at a 15s interval is ~490 samples/sec. Against a 1.4 GB TSDB over
30 days that is ~1.1 bytes per stored sample after compression, which is in the
normal range for Prometheus and so is trustworthy as a planning figure.

> **One additional always-active series costs ~190 KB of TSDB over the 30-day
> retention window** (172,800 samples × 1.1 bytes).

That gives §6 a defensible disk figure: 1,000 new series ≈ 190 MB, 5,000 ≈ 950 MB.

### Four baseline findings that change the plan

**F1 — Prometheus does not scrape itself.** There is no `prometheus` job in
`prometheus/prometheus.yml`, and `prometheus_tsdb_head_series` returns zero
results in production. Every cardinality and ingestion number in this document
had to be read from the `/api/v1/status/tsdb` endpoint by hand, because nothing
records them over time. **A cardinality abort threshold over a metric nobody
collects is unenforceable**, so Stage B must add the self-scrape job *before* the
collector, not alongside it. This is a two-line change to
`prometheus/prometheus.yml` and one entry in the expected-jobs assertion in
`tests/test_observability_config.py`.

**F2 — Server-side RED metrics already exist for all three candidate services.**
`ops`, `scraper` and `processing` each carry `prometheus-fastapi-instrumentator`
in their requirements, producing `http_request_duration_seconds` across **25
series total**, already labelled by `job`, `handler`, `method` and `status`. The
routes are **already templated in production** — the handler label reads
`/scrape_results/jobs/{job_id}/fetched`, not a raw job UUID.

The consequence is that an OTel span-metrics processor would generate a second,
overlapping copy of RED data the project already has. **The genuinely new signal
from this experiment is the client→server edge — who called whom — not request
rate, error rate or duration.** Stage B should therefore prefer the service-graph
connector and treat span metrics as optional, which also shrinks the cardinality
budget in §6.

**F3 — Docker volumes live on `/`, not `/mnt/data`.** Both
`cartracker_prometheus_data` and `cartracker_loki_data` resolve under
`/var/lib/docker/volumes`, on the 49 GB root filesystem with 27 GB free — not the
196 GB data volume with 147 GB free. Any storage this plan adds lands on the
smaller, more contended filesystem, which is the same filesystem
[Plan 135](../plans/plan_135_storage_observability.md) found filling with
unrotated container logs. The disk budget in §6 is written against `/`.

**F4 — Airflow 3.2.0 ships no OpenTelemetry packages here.** The image is
`apache/airflow:3.2.0` built from `airflow/Dockerfile` with a four-line
`requirements.txt`, and `pip list` in the running scheduler returns no
`opentelemetry-*` package at all. Airflow's native trace emission is therefore
not a configuration flip; it is a dependency addition to the largest image in the
fleet. §4 takes this out of Stage B's scope and says what is lost.

---

## 2. Selected paths

Three paths, chosen to exercise three *different kinds* of boundary rather than
three instances of the same one. Measured 24-hour request rates are included
because they drive the sampling decision in §7.

### P1 — Airflow → `ops` (HTTP, static routes)

Callers are `orphan_checker`, `scrape_listings`, `scrape_detail_pages` and
`hourly_analytics_refresh`. Seven endpoints, **none parameterized**:

| Endpoint | Requests/day |
|---|---:|
| `/maintenance/evict-delisted-cooldowns` | 287 |
| `/maintenance/reap-stuck-processing` | 287 |
| `/maintenance/expire-orphan-detail-claims` | 287 |
| `/scrape/claims/release` | 96 |
| `/scrape/claims/claim-batch` | 96 |
| `/scrape/rotation/advance` | 45 |
| `/maintenance/reconcile-cooldown-cohorts` | 23 |

Chosen because `ops` carries five of the ten declared surfaces (`detail_fetch`,
`listing_fetch`, `processing`, `archive`, `analytics`), so it is the single
service whose runtime callers matter most to Plan 142's scoped maintenance. It is
also the easy case: no path parameters means no templating risk on either side.

### P2 — Airflow → `scraper` (HTTP, **parameterized** route)

Callers are `scrape_listings` and `scrape_detail_pages`:

| Endpoint | Requests/day |
|---|---:|
| `/scrape_results/jobs/completed` | 227 |
| `/scrape_results/jobs/{job_id}/fetched` | 118 |
| `/scrape_detail/batch` | 96 |
| `/scrape_results` | 22 |

Chosen precisely *because* of `{job_id}`. Server-side templating is already
proven bounded (F2), but the **client** side has no route template — see §3,
where this turns out to be the hardest constraint in the contract.

### P3 — `processing` → MinIO and Postgres (non-HTTP, client-side only)

Chosen as the boundary with **no server-side span at all**. MinIO and Postgres
emit no OTLP here, so these edges exist only if the client asserts them. This is
the honest test of whether a client-only edge is usable evidence, and it is the
case the plan's "presence is stronger than absence" principle leans on hardest.

**Riding along at no additional design cost:** Airflow → `processing`
(`/process/batch`, 288 req/day) is structurally identical to P1 and is covered by
the same client instrumentation. It is not separately budgeted.

**Total selected-path server traffic: 0.0227 req/s — 1,961 requests/day.** For
context, all traffic to the three services including `/health` and `/ready`
probes is 0.332 req/s (28,678/day), which is dominated by Compose healthchecks.

---

## 3. Attribute contract

Identity attributes are the join to the declared graph, so they are fixed, not
conventional.

### Permitted

| Attribute | Value domain | Bound |
|---|---|---|
| `service.name` | exactly a `SERVICE_CONTRACTS` key | 34 |
| `service.namespace` | `cartracker` | 1 |
| `deployment.environment` | `production` \| `test` | 2 |
| `http.request.method` | HTTP method enum | ~5 |
| `http.route` | a registered FastAPI route template | ~25 (measured, F2) |
| `http.response.status_code` | status enum | ~10 |
| `server.address` / `server.port` | Compose service name + port | 34 / ~10 |
| `span.kind`, `otel.status_code` | enums | ~5 / 3 |
| `db.system` / `messaging.system` | `postgresql` \| `s3` (P3 only) | 2 |
| `dag_id`, `task_id` | Airflow identifiers (see §4) | ~10 / ~40 |

`dag_id` and `task_id` are admitted deliberately: they are bounded by the DAG
files in `airflow/dags/`, they already appear in
`shared/logging_setup.py::STRUCTURED_LOG_FIELDS`, and they are what recovers the
causal context that §4 gives up.

### Prohibited — dropped at the collector, not merely left unset

`url.full`, `http.url`, `http.target`, `db.statement`, `db.query.text`,
`http.request.header.*`, `http.response.header.*`, request and response bodies,
any oauth2-proxy user identity, and any attribute carrying a `listing_id`, `vin`,
`search_key`, `job_id`, `artifact_id`, `snapshot_id`, `batch_id`, or a MinIO
object key.

Enforcement is deny-by-default: the collector runs an `attributes` processor with
an explicit delete list, and a configuration test asserts that list is present and
that no exporter outside the approved set is configured. This follows the existing
pattern in `tests/test_observability_config.py`, which already asserts things like
"every retained job sets source" and "stable metric names match Grafana consumers."

### The hard constraint: client spans have no route template

This is the most likely single reason for this plan to stop, so it is stated
plainly.

Server-side instrumentation knows the matched route and emits
`/scrape_results/jobs/{job_id}/fetched`. **Client-side instrumentation does not.**
OTel's `requests` instrumentation sees only the URL string that
`scrape_detail_pages.py:100` built, which contains a live job UUID, and puts it in
`url.full`. On P2 that is a prohibited attribute by construction, and if
`url.full` is also used for span naming, the span *name* becomes unbounded —
which the attribute delete list does not fix.

P2 is therefore admissible only if client spans are named from a template supplied
at the call site, with `url.full` deleted at the collector. If that cannot be done
cleanly, **P2 must be dropped rather than sampled or redacted after the fact**,
and the experiment proceeds with P1 and P3 alone.

---

## 4. Propagation contract and its gaps

W3C `traceparent` propagates over the HTTP calls in P1 and P2.
`shared/logging_setup.py` already reserves `trace_id` and `span_id` in
`STRUCTURED_LOG_FIELDS`, but **nothing in the repository populates them** — those
two names appear only in that module and its test. The fields are schema-ready and
data-empty. Stage B may populate them;
[Plan 141](../plans/plan_141_structured_log_ingestion_contract.md) owns what they
mean.

Three places where propagation cannot be continuous, recorded as the stage item
requires:

**G1 — The Airflow task boundary is not crossed.** DAG code calls services with
bare `requests`; a trace started inside a task does not link to the DAG run unless
Airflow emits its own spans, and per F4 the image has no OTel packages. Adding
them means changing `airflow/requirements.txt`, rebuilding the fleet's Airflow
image, and giving it the isolated-venv treatment in CI that `apache-airflow`
already requires. **Decision: out of scope for Stage B.** Each task's HTTP call is
its own trace root. What is lost is the ability to say *which DAG run* caused an
edge; what recovers most of it is carrying `dag_id` and `task_id` as attributes
(§3), which is bounded and cheap.

**G2 — The scrape path's async handoff breaks the trace.** `scrape_detail/batch`
returns immediately and results are collected later by polling
`/scrape_results/jobs/completed`. Submit and poll are separate traces with no
carrier between them, and adding one means threading a trace context through the
job store — a production write path, for telemetry. Out of scope. Consequence: the
scrape path appears as two unrelated edges, not one flow.

**G3 — P3 has no server side.** MinIO and Postgres do not emit OTLP here, so those
edges are client-asserted only. An edge is evidence that the client tried, not that
the server saw it.

---

## 5. The declared-graph export — and the finding that blocks it

The stage item requires the declared graph to be *derived from* Plan 142's registry
rather than duplicated. The registry is
`ops/coordination_contract.py::SERVICE_CONTRACTS` — 34 services, each carrying
`surfaces`, `lifecycle`, `compose_dependencies` and `followers`, with exact
coverage against `docker-compose.yml` enforced in CI. Its keys are the same strings
Compose uses, which makes them the natural value for `service.name` and the join
key for any comparison.

Extracting edges from it yields **37 `compose_dependencies` edges** plus 4
`followers` edges and 50 service×surface pairs. And extracting them surfaces the
problem:

```text
does ANY service declare a dependency on ops / scraper / processing?
  ops          declared-by: ['caddy']
  scraper      declared-by: NOBODY
  processing   declared-by: NOBODY

declared compose_dependencies for the Airflow services:
  airflow-apiserver      deps=['airflow-init', 'postgres']
  airflow-dag-processor  deps=['airflow-init', 'postgres']
  airflow-scheduler      deps=['airflow-init', 'postgres']
  airflow-triggerer      deps=['airflow-init', 'postgres']
```

**`compose_dependencies` models container startup order, not runtime call
topology.** Airflow calls `ops`, `scraper` and `processing` 1,961 times a day (§2)
across edges that appear nowhere in any service's contract. `ops` is declared only
by `caddy`, its reverse proxy. `scraper` and `processing` are declared by nobody at
all.

So if Stage C's comparator were built against `compose_dependencies` as written,
**all three selected paths would report as "observed but undeclared" on day one** —
and that would not be registry drift, which is what the alert is supposed to mean.
It would be a category error baked into the comparison. Plan 151's own architecture
section anticipates exactly this when it warns that the comparator "must distinguish
service identity, operational surface, and transport edge," and that "a network call
proves communication, not that both endpoints mutate the same state."

This is a genuine Stage A finding: the declared graph does not currently have a
stable export **of the thing the experiment wants to compare against**, because the
registry does not model call edges at all.

The obvious repair is to add a `runtime_calls` field to `ServiceContract`. That was
rejected on the correct objection: a hand-maintained edge list is a second copy of
something the code already knows, and a second copy is how the first one goes stale
— the same argument `maintenance-running-set.txt` makes about itself when it refuses
to list all 26 default services. So the question became whether the call graph is
*derivable* rather than maintained. It is, and answering that question is what
decided the plan.

### 5.1 The call graph is derivable, and deriving it is most of the plan's value

A prototype deriver reads two static sources and needs no hand-maintained list:

1. **Compose `environment:` blocks** — `SCRAPER_URL: http://scraper:8000`,
   `MINIO_ENDPOINT: http://minio:9000`, `PGHOST: postgres`,
   `DATABASE_URL: postgresql://…@postgres:5432/…`, `REDIS_URL: redis://redis-trawl:6379`
2. **Literal service URLs in each service's own source** — the
   `OPS_URL = "http://ops:8060"` constants in `airflow/dags/`, the
   `SERVICE_EVIDENCE` table in `ops/coordination_drain.py`, and so on

In ~80 lines it produces **77 call edges**, against the registry's 37
`compose_dependencies`, and it independently recovers all five edges of the three
paths selected in §2.

| Relation | Edges |
|---|---:|
| Derived call edges | **77** |
| Declared `compose_dependencies` | 37 |
| Derived but **undeclared** | **62** |
| Declared but not derived | 22 |

The 62 undeclared edges are real and specific: `ops → prometheus`, `ops → loki`,
`ops → grafana`, `ops → container-health`, `ops → {scraper, processing, archiver,
dbt_runner, pack-worker}`, `dashboard → postgres`, `scraper → postgres`, and all
four Airflow services to six application services.

**This is the finding Plan 151 budgeted an entire OTLP pipeline and a seven-day
soak to produce.** Success criterion 4 asks for *one* deliberately created
observed-but-undeclared edge to be detected. Static derivation produced 62
undeliberate real ones, in CI, deterministically, at zero runtime cost.

#### What the deriver misses, and why it matters

The 22 declared-but-not-derived edges are where the counter-argument would live, so
they were checked individually:

- **11 are correctly not call edges.** `X → flyway` and `X → airflow-init` are
  startup ordering — waiting on a migration to complete is not a call.
- **11 are real calls the prototype missed:**
  `caddy → {ops, dashboard, grafana, airflow-apiserver, oauth2-proxy}`,
  `grafana → {loki, prometheus}`, `promtail → loki`, `flyway → postgres`.

Every one of those 11 is missed for the same reason: its configuration lives in a
Caddyfile, a Grafana datasource YAML, a promtail YAML, or Flyway command arguments
rather than in Python or Compose env. **All 11 are in checked-in files.** None
requires runtime observation to find; the deriver needs one more config format
taught to it, not a collector. Static coverage here is bounded by how many
configuration languages the deriver reads, not by what is knowable without running
the system.

#### What runtime observation would still uniquely answer

Stated fairly, because the decision in §10 depends on it being stated fairly:

1. **Which of the 77 edges actually fire** — a live dependency versus dead code.
   Static analysis genuinely cannot answer this, and it is real operational value.
2. **Edges that appear in no checked-in file** — targets constructed at runtime.

The weight of (2) is small *here specifically*, and this is an argument rather than
a measurement: this project's service discovery is entirely static Compose DNS.
There is no service registry, no dynamic routing, and no configuration loaded from
the database into a call target. The space in which runtime could surprise the
deriver is correspondingly narrow.

---

## 6. Resource budget and abort thresholds

Recorded before deployment, as the stage item requires. Breaching an abort
threshold rolls back Stage B; it does not weaken Plan 142 policy.

| Resource | Budget | Abort threshold | Basis |
|---|---|---|---|
| Collector memory | 384 MiB | sustained > 400 MiB; hard `mem_limit` 512 MiB | Prometheus itself runs at 188.7 MiB; a collector over these paths should not exceed it |
| Collector CPU | 0.25 core | sustained > 0.5 core (12.5% of host) | host runs at ~1.0 of 4 cores; 0.5 keeps total under 40% |
| **New Prometheus series** | **1,500** | **> 2,500** | +20% on the measured 7,347. F2 means the service graph alone should need far less |
| TSDB growth | +300 MB / 30d | > +600 MB | 1,500 × 190 KB ≈ 285 MB, from the §1 marginal figure |
| Disk | on `/` only | `/` crosses **60%** used | F3: volumes are on the 27 GB-free root filesystem, currently 45% |
| Span ingestion | ≤ 5 spans/s | sustained > 25 spans/s | §2 measures 0.0227 req/s of real traffic; even counting healthchecks it is 0.332 req/s |
| Network | intra-host only | any non-Compose-network egress | no telemetry leaves the host under this plan |

The span-ingestion abort threshold is deliberately three orders of magnitude above
measured traffic and still tiny. That asymmetry is the point: at this volume, a
breach means a misconfiguration — auto-instrumentation picking up healthchecks or
Airflow's internal HTTP — not growth.

**Enforceability depends on F1.** The series and TSDB thresholds require
`prometheus_tsdb_head_series` and `prometheus_tsdb_storage_blocks_bytes`, which
nothing collects today. Stage B adds the self-scrape job first.

---

## 7. Sampling policy

**Head sampling at 100% (`always_on`) for the selected paths, with healthchecks
excluded from instrumentation entirely.**

Justification is the measurement, not a preference. Real selected-path traffic is
**0.0227 req/s — 1,961 requests/day**. At 5 spans/s the budget in §6 has ~200×
headroom over 100% sampling. Sampling would buy nothing measurable and would cost
correctness.

**What sampling would make unsafe, recorded for the record.** This experiment's
primary signal is edge *presence*. Plan 151 already treats "declared but not
observed" as weak evidence; any sample rate below 100% weakens it further, and on a
low-rate path it destroys it. `/maintenance/reconcile-cooldown-cohorts` runs 23
times a day — at 10% sampling that edge has a real chance of being invisible over a
full day, and Stage E's seven-day window would record a false absence.

The rule that follows: **if traffic ever grows enough to force sampling, the plan
reconsiders its scope rather than lowering the rate.** Narrowing the instrumented
set keeps every retained edge trustworthy; sampling makes every edge slightly
untrustworthy, which is the worse trade for a topology audit.

Healthcheck exclusion matters for the same reason: `/health` and `/ready` are 93%
of all requests to these three services (0.332 vs 0.0227 req/s), carry no
topological information — Compose is the caller, not another service — and would
dominate every panel.

---

## 8. Stage B contract

**Not executed** — §10 declined Stage B. Retained because the stage item requires
it to be written, because the decision to decline is only credible if what was
being declined is specified, and because it is the starting point if the trigger in
§10 ever fires.

**Dashboard** — one Grafana view, provisioned as code alongside the existing files
in `grafana/provisioning/`:

1. Observed edge list — client, server, last-observed timestamp
2. Declared-vs-observed status per edge, once §10 resolves
3. Request rate and error rate per edge
4. Collector health: uptime, queue depth, refused/dropped spans, export failures
5. Instrumentation coverage — which services emit at all
6. Live series count against the 1,500 budget from §6

**Alerts** — following the existing rule pattern in
`grafana/provisioning/alerting/rules.yml`:

| Alert | Condition |
|---|---|
| Collector down | `up{job="otel-collector"} == 0` for 10m |
| Span drops | any sustained refusal/drop rate for 15m |
| Export failures | sustained export failure rate for 15m |
| Cardinality budget | new series > 1,500 for 30m |
| Series abort | new series > 2,500 — pages |

No alert fires on an observed-but-undeclared edge in Stage B. That is Stage C's,
and it requires the bounded persistence window the plan already specifies so a
one-off startup call cannot page.

**Verification** — each is a demonstration, not an assertion:

1. Prometheus self-scrape lands first (F1) and the budget metrics are readable
2. Every selected path produces the expected edge under real DAG traffic
3. **No prohibited attribute reaches Prometheus** — asserted against live series
   labels, not against the config alone
4. `http.route` label values match registered FastAPI routes exactly
5. Client span names on P2 are templated (§3) — the specific check that admits or
   drops P2
6. **Stopping the collector does not fail a DAG run.** The plan requires telemetry
   to fail open, so it is proven by stopping the collector during a live run, not
   by reading the code
7. Measured cost recorded against every §6 row

**Rollback** — in order, each independently sufficient:

1. Stop the collector. Applications keep running (verification 6 proves this).
2. Remove the OTLP exporter env from instrumented services and redeploy per the
   per-image service list — `shared/` changes mean
   `archiver pack-worker processing scraper dbt_runner dashboard`, then `ops` last
   and alone.
3. Remove the collector service from `docker-compose.yml`, its Prometheus job, its
   dashboard and its alert rules.
4. Delete the collector's series from Prometheus, or let 30d retention age them out.

Nothing in the rollback touches `ops/coordination_contract.py`, Plan 142's
coordination state, or the running set. That separation is the plan's core safety
property and it holds in reverse too.

---

## 9. How these numbers were taken

All reads were read-only, from the production VM. Prometheus publishes no host
port, so queries went through `docker exec`:

```sh
# host
uptime; nproc; free -m; df -h / /mnt/data; uname -m
docker stats --no-stream
docker inspect cartracker-prometheus --format '{{json .Args}}'
docker inspect cartracker-prometheus --format '{{range .Mounts}}...{{end}}'

# prometheus
docker exec cartracker-prometheus wget -qO- \
  "http://localhost:9090/api/v1/status/tsdb"
docker exec cartracker-prometheus wget -qO- \
  "http://localhost:9090/api/v1/targets?state=active"
docker exec cartracker-prometheus wget -qO- --post-data \
  'query=count by (job) ({__name__!=""})' \
  "http://localhost:9090/api/v1/query"
docker exec cartracker-prometheus wget -qO- --post-data \
  'query=sort_desc(sum by (job, handler) (rate(http_request_duration_seconds_count{...}[24h])))' \
  "http://localhost:9090/api/v1/query"
docker exec cartracker-prometheus du -sh /prometheus
docker exec cartracker-loki du -sh /loki

# airflow
docker exec cartracker-airflow-scheduler airflow version
docker exec cartracker-airflow-scheduler pip list | grep -i opentelemetry
```

The declared-graph counts in §5 come from importing `SERVICE_CONTRACTS` directly
rather than parsing it, so they reflect what the code actually builds.

---

## 10. The decision: no telemetry pipeline

**Decided 2026-09-08. Stages B through E do not run.**

Plan 151 asked two questions. Stage A answered both without deploying anything.

> *Does runtime topology expose meaningful undeclared dependencies or improve
> incident diagnosis beyond the existing metrics and logs?*

It would expose undeclared dependencies — but so does static derivation, which
found 62 of them for ~80 lines and no operating cost (§5.1). The marginal
contribution of the runtime pipeline is not "finds undeclared edges"; it is "tells
you which edges are live," which is a materially smaller claim than the one the
plan was scoped against.

> *Is metrics-only trace processing sufficient, or does retaining and querying
> individual traces justify operating Tempo?*

Moot. Neither is built.

Three measurements decided it, and the cost side matters as much as the benefit
side:

1. **The benefit is largely obtainable statically** (§5.1) — and obtainable
   *better*: in CI, deterministically, with complete coverage independent of
   traffic. Plan 151 concedes that "declared but not observed" is weak evidence
   because an edge may be idle, rare or sampled. Static derivation has no such
   weakness. On the plan's own primary signal, the cheap method is the more
   trustworthy one.
2. **Half of what the pipeline would emit already exists** (F2). All three
   candidate services already publish route-templated RED metrics. The new signal
   was only ever the edge.
3. **The remaining unique value is narrow** (§5.1). Liveness of an edge, on a
   fleet whose service discovery is entirely static Compose DNS.

Against that: a new always-on collector service, an OTLP dependency in every
instrumented image, an Airflow image rebuild if propagation is ever to be
continuous (F4), a cardinality budget that is not even enforceable until Prometheus
is made to scrape itself (F1), and a seven-day observation window — to answer a
question that is mostly already answered.

This is Plan 151's **Stage E "remove"** outcome — "remove the pipeline because its
value does not justify its cost" — reached at Stage A. The plan is explicit that
this is a success: *"a decision of remove is a successful exit, not a failed one --
this plan is allowed to conclude no, and the exit is the record, not the pipeline
surviving."*

### What Stage A produced instead

The durable output is this document plus three things worth acting on
independently of the plan that found them:

1. **The derived call graph is worth building as a CI check.** It is the
   cheap majority of the value Plan 151 was chasing: 62 undeclared edges today,
   enforced continuously, deriving from the same checked-in files it audits so it
   cannot drift. It requires no change to `ops/coordination_contract.py`, which
   keeps Plan 142's closeout undisturbed. Scoping it is a separate decision — it is
   implementation work, and Stage A's exit is a document.
2. **F1 — Prometheus does not scrape itself.** A live observability gap unrelated
   to this plan. Its own TSDB size, series count, ingestion rate and memory are
   uncollected, so no alert can be written over any of them and every number in §1
   had to be read by hand.
3. **F3 — Docker volumes sit on the 27 GB root filesystem**, not the 147 GB data
   volume, on the same filesystem Plan 135 already found filling with container
   logs.

### What would justify revisiting this

A concrete diagnostic question that the derived graph and existing metrics
demonstrably cannot answer — most plausibly an incident where the sequence of
cross-service calls, not their existence, is what is in dispute. Absent that, the
recorded position is that this project does not need distributed tracing, and
§§1–8 are the costing that supports it.

---

## Stage A gate

> Proceed only if the selected paths can be instrumented without sensitive or
> unbounded attributes, the declared graph has a stable export, and the collector
> fits a written single-host resource budget.

| Condition | Verdict |
|---|---|
| Paths instrumentable without sensitive or unbounded attributes | **Yes, conditionally.** P1 and P3 are clean. P2 is admissible only if client spans are templated at the call site (§3); if not, P2 drops and the experiment proceeds with two paths |
| Declared graph has a stable export | **Yes, via derivation** (§5.1) — but not the export the plan assumed. `compose_dependencies` exports startup order; the call graph the comparator needs is derived from Compose env and source constants, and needs no change to `ops/coordination_contract.py` |
| Collector fits a written single-host resource budget | **Yes.** §6 is written with abort thresholds. Headroom is large: 17.4 GiB RAM free, ~3 of 4 cores idle, measured traffic 200× under the ingestion budget. One dependency: F1, the self-scrape job, must land before the thresholds are enforceable |

**All three gate conditions are met, and the plan stops anyway.**

That is not a contradiction; it is the distinction between *can* and *should*. The
gate asks whether the experiment is safe and affordable to run. It is. §10 asks
whether it is worth running, and the answer is no — because Stage A also discovered
that the experiment's principal finding is available without it (§5.1), which is
not a question the gate was written to ask.

Recording it this way keeps both facts recoverable. If the trigger in §10 ever
fires, the pipeline is known to be feasible inside a written budget and this
document is the design; what changed would be the value side, not the cost side.
