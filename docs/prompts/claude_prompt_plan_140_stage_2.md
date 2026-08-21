# Claude Prompt: Plan 140 Stage 2 — the container-health metric and its alerts

You are working in the `cartracker-scraper` repo. Branch off `master`.

Read `docs/plan_140_service_health_contract.md` first — it is the source of
truth. The sections that matter most are **"Unconfigured must be loud"**,
**"Stage 1 — Healthchecks everywhere"** including its 2026-08-20 soak record and
the sibling-project finding, and **"Stage 2 — The metric and the alert"**. Then
read `docs/plan_135_storage_observability.md`, "Stage 4 as built" — this stage is
a second producer into that exact plumbing, not a new exporter.

## Where this stands

**Stages 1 and 3 are complete and verified. Stage 2 has not started.**

| Stage | State |
|---|---|
| 1. Healthchecks everywhere | ✅ 25 of 25 configured checks healthy, zero failing streaks, soak closed 2026-08-20 at 46 hours with zero false pages |
| 2. Metric + alerts | ❌ **this session** |
| 3. CI asserts coverage | ✅ `TestServiceHealthCoverage` in `tests/test_observability_config.py` |
| 4. Retire DAG sensors as the health signal | ❌ deliberately last; needs Stage 2 alerts first |

You are starting from a clean baseline: as of 2026-08-20 the host reports **zero
unhealthy containers**. That matters for the "prove it stays quiet on healthy
data" half of the validation below.

---

## The metric

```
cartracker_container_health{container="ops"}     1   # healthy
cartracker_container_health{container="trawl"}   0   # unhealthy
cartracker_container_health{container="caddy"}  -1   # NO HEALTHCHECK CONFIGURED
```

Three states, not two. Collapsing `-1` into "absent" is how a monitoring gap
disguises itself as a healthy system. It is ugly on a graph on purpose.

Serve it from a **small dedicated exporter service** that computes the values in
its `/metrics` handler, and add a `container-health` scrape job to
`prometheus/prometheus.yml`.

---

## Three decisions to make deliberately, with recommendations

### 1. Mechanism — a dedicated exporter, **not** the textfile collector

The plan doc originally specified node-exporter's textfile collector, and was
**amended on 2026-08-20**. Read the "Amended" subsection under Stage 2 before
you start; the short version is below.

Plan 135 uses the textfile collector because a `du -s -x` walk took 456 seconds.
That cannot happen inside a scrape handler, so it must run ahead of time and
leave a file. Container health is one Docker API call in the low tens of
milliseconds — the opposite case, and cheap enough to compute when Prometheus
asks.

Computing at scrape time deletes work rather than adding it:

- **Staleness becomes structurally impossible.** No `.prom` file, no
  carry-forward, no companion timestamp metric, no staleness alert. A whole
  category of defect — the one Plan 143 spent a 24-hour soak correcting and
  Plan 136 D2 named before it — simply cannot occur.
- **The exporter's own liveness is `up{job="container-health"}`**, free.
- **The socket grant lands on a container with no other credentials.**

Do **not** bundle this into `pack-worker`. It was the obvious host — it already
carries the writable `node_textfile` mount — and it is wrong on four counts: it
is a batch worker for bronze packing and this is unrelated work; it holds
Postgres and MinIO credentials that a health reader has no business near; it
would report on its own health, so a wedged pack-worker reads as a healthy
fleet; and every Plan 131/132 change would redeploy your monitoring.

`scraper/app.py:106` has the `lifespan` pattern if you need app scaffolding;
there is no `threading.Thread` precedent in these services, so do not invent one.

### 2. Socket access — **use `docker-socket-proxy`, and know why `:ro` is not enough**

Plan 136 Stage 4 proposes `docker-socket-proxy` for restart authority and says
"if that lands first, read health through it. **Do not add a second socket
path.**" It has not landed — Plan 136 Stages 2-4 are unstarted — so this stage
is the one that introduces socket access, and it should introduce the shape
Stage 4 can extend.

**Do not reach for `- /var/run/docker.sock:/var/run/docker.sock:ro` and call it
read-only. It is not.** The `:ro` flag makes the socket *file* read-only; it does
nothing to the Docker API reachable through it. Any client that can connect can
issue `POST /containers/{id}/restart`, `kill`, or create a privileged container.
`pack-worker` already holds Postgres and MinIO credentials, and widening it to
full Docker API access to read a health string is a bad trade.

`docker-socket-proxy` with `CONTAINERS=1` and `POST=0` is the only option that
actually enforces read-only, and it gives Plan 136 Stage 4 a narrow, explicit
place to later grant exactly one verb.

**Note in passing:** `promtail` already has `/var/run/docker.sock:...:ro` at
`docker-compose.yml:951` for log-discovery metadata, which is the same
full-API grant. That is pre-existing and **out of scope** — do not fix it here —
but say so in the plan doc so it is not mistaken for a precedent that settles
this decision.

### 3. Scoping — the default `cartracker` project's **running** services

This is the finding from the 2026-08-20 soak and it is easy to get subtly wrong.

Four containers — `cartracker-lakekeeper`, `-lakekeeper-migrate`,
`-lakekeeper-postgres`, `cartracker-mlflow` — were stopped and holding a stale
`unhealthy` state. They were removed on 2026-08-20, but **they belong to two
live sibling compose projects** (`docker-compose.lakehouse.yml`,
`docker-compose.mlflow.yml`) supporting Plans 125 and 112, and `up -d` brings
them straight back.

So:

- Enumerating `docker ps -a` publishes permanent `0`s for deliberately-stopped
  containers and pages forever.
- Filtering to "compose-managed" does **not** help — those containers are
  compose-managed. That filter would not have excluded a single one of them.
- Scope to the running services of the **default `cartracker` project**, via the
  `com.docker.compose.project` label, and apply `-1` only to services
  `docker-compose.yml` itself declares.

Do not let today's cleanup satisfy this. The requirement stands on its own
precisely because the next `up` of either sibling project recreates the
condition.

---

## Fix `ct-service-down` in this session

The scrape-time design leans on `up{job="container-health"}` as the exporter's
liveness signal. That only works if something alerts on `up`.

`ct-service-down` currently selects `up{job=~"ops|processing"}` — **an allowlist
covering two of eight scrape jobs.** That is the same defect this entire plan
exists to close, sitting in `grafana/provisioning/alerting/rules.yml:197`, and
it means `airflow`, `postgres`, `minio`, `minio_bucket`, `dbt_runner`, and `node`
are all currently unwatched at the target level.

Widen it to cover every job in `prometheus/prometheus.yml`, and add a coverage
test asserting the alert's job set equals the scrape config's job set — the
exact-set-equality style `tests/test_observability_config.py` already uses for
the Promtail job set. Without that test this row rots again the next time
someone adds a scrape target.

---

## The alerts

Add to `grafana/provisioning/alerting/rules.yml`:

- **`ct-container-unhealthy`** — any `0` for 5m. A real incident.
- **`ct-container-health-unconfigured`** — any `-1`. **Not** an incident; a
  *coverage* alert, routed accordingly. It should read as "this plan regressed."

There is deliberately **no staleness rule**. The exporter computes at scrape
time, so it cannot serve a stale value; its liveness is the widened
`ct-service-down` above. If you find yourself adding a freshness metric here,
stop — it means the design drifted back toward a cached file.

Validate **in both directions**, the way Plan 131 did for
`ct-pack-verification-refused` (`rules.yml:94`):

1. prove the selector matches live series;
2. prove the expression stays quiet on healthy data — you have a clean baseline
   for this right now;
3. prove it fires, using a **deliberately stopped non-critical container**, never
   by breaking something real.

When fire-testing, hold the condition past `group_wait` (30s) or the alert
cancels its own notification. That is recorded in
`docs/runbook_storage_maintenance.md` and was learned the hard way.

---

## Tests

Extend `tests/test_observability_config.py` — it already owns the alert-contract
assertions (`test_metrics_freshness_alert_uses_the_plan_143_contract` at :945 and
`test_pack_verification_refused_alerts_on_any_occurrence` at :972 are the
patterns to copy):

- the two new UIDs exist and parse;
- the unhealthy rule selects `0` and the unconfigured rule selects `-1`, so a
  future edit cannot silently merge them;
- the `container-health` scrape job exists in `prometheus/prometheus.yml`;
- **`ct-service-down`'s job set equals the scrape config's job set**, by exact
  set equality — this is the test that stops the allowlist from re-rotting;
- **a test pinning the scoping rule** — the assumption most likely to be
  loosened later by someone "simplifying" the collector. Pin that the project
  label is required, not that the current four sibling containers are absent.

Unit-test the metric renderer against a fixture containing all three states plus
a sibling-project container that must not appear.

---

## Deploy notes

`archiver/Dockerfile` does `COPY . .`, and `archiver` and `pack-worker` share the
`cartracker-archiver` tag. Build once, recreate both:

```bash
docker compose build archiver
docker compose up -d node-exporter pack-worker archiver
docker exec cartracker-pack-worker ls /app/archiver/processors/   # verify what loaded
```

Adding `docker-socket-proxy` is a **compose change**, so `docker restart` will
not apply it — that needs `docker compose up -d`. The reverse holds for
bind-mounted file contents. Verify with
`docker inspect --format '{{json .Args}}'`, never container uptime.

Declare deploy intent through the admin UI and let the system drain first, but
**build before declaring it** — every DAG's `deploy_intent_sensor` has
`timeout=600`, so a slow build inside the window fails their first task.

---

## Decisions already made — do not relitigate

- **Three states, with `-1` explicit.** Absence is the failure mode being fixed.
- **A dedicated exporter computing at scrape time**, amended 2026-08-20. The
  textfile collector is right for a 456-second disk walk and wrong for a 10ms
  API call; see the Stage 2 "Amended" subsection for the full argument.
- **Not cAdvisor.** Heavier, and its health semantics are less direct than
  `.State.Health.Status` (Plan 136, "0b").
- **Deny-list, never allowlist.** `TestServiceHealthCoverage` already encodes
  this and every entry carries a written reason.
- **`oauth2-proxy` stays the documented exception.** Its distroless image has no
  shell, curl, wget, or busybox. Swapping the authenticated front door to
  `latest-alpine` is a separate deploy decision — not this session.
- **`trawl` is not fixed by this stage.** It has a healthcheck, it returned
  `status:ok` for all eight hours of the 2026-08-14 outage at a 0% solve rate,
  and it showed `(healthy)` throughout. That is an *efficacy* defect and it
  belongs to Plan 136 Stage 2. Do not deepen the probe into a real solve —
  Plan 136 Stage 4 prices that at 30-90s per interval against cars.com and
  rejects it.
- **No automatic restart.** This stage produces the signal. Plan 136 Stage 4
  owns restart authority and the argument for who may call it.

## What would make this session wrong

- Publishing the metric from an Airflow DAG, so it goes blind exactly when
  Airflow is the thing that broke.
- Bundling the collector into `pack-worker` because it already has the textfile
  mount. That is how a bronze-packing batch worker ends up owning fleet health
  and reporting on itself.
- Writing a `.prom` file anyway, then discovering you need a staleness metric
  and a staleness alert to make it safe. That is the design you were amended
  away from.
- Mounting `docker.sock` with `:ro` and describing it as read-only in a comment
  or commit message.
- Enumerating `docker ps -a`, or scoping to "compose-managed" and believing that
  excludes the sibling projects.
- Leaving `ct-service-down` as an allowlist while depending on `up` for the
  exporter's liveness.
- Treating the 2026-08-20 orphan cleanup as having solved the scoping problem.
- Suppressing `-1` because it is noisy. It is noisy **by design**; suppressing it
  inverts the plan.
- Fire-testing by breaking something real instead of stopping a non-critical
  container.
- Reading Stage 1's completion as the coverage problem being solved. Stage 1
  fixed *missing* signals. It did nothing about signals that lie.

## Related, not in scope

- **Plan 136 Stage 2** (solver outcome counters) is the next row after this and
  is the efficacy layer no healthcheck can supply. It will want the socket proxy
  this stage introduces.
- **Plan 141** owns the logging source-policy registry and the
  `ct-403-log-spike` false positive (a bare `|= "403"` matching UUIDs and sha256
  prefixes). Health coverage and logging coverage have deliberately different
  inclusion policies; do not merge them.
- **Plan 142** consumes this stage's health coverage as its mandatory resume
  gate for whole-host maintenance.
