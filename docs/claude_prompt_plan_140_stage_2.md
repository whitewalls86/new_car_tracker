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

Emit it through node-exporter's textfile collector. The plumbing already exists
and is proven: `node_textfile` is mounted writable on `pack-worker` and `:ro` on
`node-exporter`, which runs with `--collector.textfile.directory=/textfile`.
Follow `archiver/processors/disk_usage.py` for the file conventions — atomic
write via `tempfile.NamedTemporaryFile` in the same directory then `os.replace`,
and explicit `# HELP` / `# TYPE` lines.

Write a **separate `.prom` file** from `cartracker_disk_usage.prom`. The
node_exporter#1885 hazard the disk-usage module documents is about splitting a
single metric *family* across files; two different families in two files is
fine and is the cleaner separation here.

---

## Three decisions to make deliberately, with recommendations

### 1. Who writes it, and how often — **not an Airflow DAG**

`disk_usage` is triggered by a daily DAG hitting `POST /disk-usage/run`, and
copying that pattern here is the obvious move and the wrong one.

**A container-health metric must not depend on Airflow, because one of the two
incidents this plan exists for was Airflow dying.** On 2026-08-18 the
`airflow-apiserver` connection pool failed. A health metric published by an
Airflow DAG goes stale exactly when it is most needed, and — per Plan 143's
finding about silent staleness — a stale gauge scrapes as a live one.

Recommendation: a **background refresh loop inside `pack-worker`**, on a ~30s
period. It already carries the writable `node_textfile` mount and
`DISK_USAGE_TEXTFILE_DIR`, it is long-running, and it has no Airflow dependency.
`scraper/app.py:106` has the `lifespan` pattern to follow; there is no
`threading.Thread` precedent in these services, so prefer `lifespan` with an
asyncio task over inventing one.

Gate it on an env var so only `pack-worker` runs it, the way
`ARCHIVER_ALLOW_PACK_JOBS` gates the pack jobs — `archiver` and `pack-worker`
share the `cartracker-archiver` image, and two writers racing on one `.prom`
file is a defect even with atomic replace.

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

## Staleness is mandatory, not a nice-to-have

If the refresh loop dies, the `.prom` file keeps its last contents and
node-exporter keeps serving them. Every container then reads healthy forever.
That is the exact failure Plan 143 spent a full soak correcting, and Plan 136 D2
before it.

Publish a companion timestamp — follow
`cartracker_disk_usage_measured_timestamp_seconds` in
`archiver/processors/disk_usage.py:51` — and alert on it. A health metric that
cannot go stale-loud is not finished.

---

## The alerts

Add to `grafana/provisioning/alerting/rules.yml`:

- **`ct-container-unhealthy`** — any `0` for 5m. A real incident.
- **`ct-container-health-unconfigured`** — any `-1`. **Not** an incident; a
  *coverage* alert, routed accordingly. It should read as "this plan regressed."
- **A staleness rule** on the timestamp above, following Plan 143's cadence-aware
  shape: `noDataState` and `execErrState` must fail loudly, and the threshold
  should be a small multiple of the refresh period, not a round number someone
  liked. Plan 143's soak found a 900s threshold against an hourly publisher;
  do not repeat that class of mistake in the other direction.

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

- the three new UIDs exist and parse;
- the unhealthy rule selects `0` and the unconfigured rule selects `-1`, so a
  future edit cannot silently merge them;
- the scrape/collector wiring is asserted the way the Plan 135 textfile
  contract is;
- **a test pinning the scoping rule** — the assumption most likely to be
  loosened later by someone "simplifying" the collector. Pin that the project
  label is required, not that the current four sibling containers are absent.

Unit-test the renderer against a fixture containing all three states plus a
sibling-project container that must not appear.

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
- **Textfile collector, not cAdvisor.** cAdvisor is heavier and its health
  semantics are less direct than `.State.Health.Status` (Plan 136, "0b").
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
- Mounting `docker.sock` with `:ro` and describing it as read-only in a comment
  or commit message.
- Enumerating `docker ps -a`, or scoping to "compose-managed" and believing that
  excludes the sibling projects.
- Shipping without a staleness signal, so a dead writer reads as a healthy fleet.
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
