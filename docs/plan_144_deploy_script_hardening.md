# Plan 144: Deploy Script Hardening

## Status

**Implemented 2026-08-20, not yet deployed.** Priority **78 (high)**. Effort
**XS** (hours to 1 day).

Small, concrete, and unblocked as of 2026-08-18. Every defect below was observed
during a real production deploy on 2026-08-20 — defects 1-3 during Plan 133's,
defect 4 during Plan 140 Stage 2's, defect 5 during Plan 140 Stage 1's, two days
after the fact.

What landed:

| File | What it is |
|---|---|
| `scripts/redeploy.sh` | `--no-deps`, a real health gate, a split intent-release rule, a `--restart` mode for changes Compose cannot see, and a no-op report when a recreate recreated nothing |
| `healthcheck-exemptions.txt` | The Plan 140 deny-list, moved out of the test file so the deploy poller and `TestServiceHealthCoverage` read one list |
| `deploy-followers.txt` | Services whose peers cache their address, and what to restart after recreating them |
| `tests/test_deploy_script.py` | The invariants none of the above can check at runtime |

**Verification against production is still owed** — see
[Verification](#verification). The code is on the VM at `1b66213` and its
assumptions were checked there read-only, but no deploy has run.

## Why this exists now

`scripts/redeploy.sh` is the deploy path for every service in this system. It
works, and it had three defects — six by the time anyone finished looking —
that were tolerable while nobody looked closely and are not tolerable now that
[Plan 142](plan_142_planned_host_maintenance.md) intends to build a whole-host
maintenance procedure on top of it.

The trigger is that **[Plan 140](plan_140_service_health_contract.md) Stage 1
made the main fix possible.** The script carries this comment:

```bash
# TODO Plan 76: replace sleep with health endpoint polling
sleep 10
```

That TODO could not be actioned when it was written: only 7 of 31 services had a
healthcheck, so there was nothing uniform to poll. As of PR #216 all 25 in-scope
services have one. The blocker cleared on 2026-08-18 and nothing noticed.

**The reference is also stale.** Plan 76 is *complete* — it closed 2026-03-30 as
"Service Health Gate," and it never owned this TODO. A comment pointing at a plan
that finished five months ago is worse than no comment: it reads as tracked work.

## The defects

They were three when this was drafted. Two more arrived from production before
a line was written, and both are the same shape as each other: **a deploy action
with an invisible side effect on a service the operator did not name.** A sixth
arrived hours after the first five shipped, and is the same shape again — this
time in the fix itself.

### 1. `docker compose up -d` without `--no-deps`

```bash
docker compose up -d "$@"
```

[ARCHITECTURE.md](ARCHITECTURE.md) documents the deployment flow as
`docker compose up -d --no-deps <service>`. The script omits it, so Compose walks
the dependency graph.

**Observed 2026-08-20:** deploying `ops archiver pack-worker processing` re-ran
`flyway`. It was harmless — 43 migrations validated, none pending, exit 0 — but
it is unintended work inside a deploy window, it will recur on every deploy, and
it means the blast radius of a deploy is larger than the service list implies.

The fix is `--no-deps`, plus a decision recorded here about whether dependency
health should be *checked* rather than *recreated*.

### 2. `sleep 10` instead of health polling

Ten seconds is not a readiness contract. It is longer than some services need and
shorter than others: `loki` and `pgadmin` were both observed reporting
`health: starting` past that mark during the Plan 140 Stage 1 deploy.

A deploy that returns "Done." while a service is still starting is a deploy that
cannot tell success from a slow failure. Poll
`docker inspect --format '{{.State.Health.Status}}'` until every recreated
service is `healthy` or a bounded timeout expires, and **fail loudly on timeout**
rather than proceeding.

Services on the Stage 3 deny-list have no health status by design — `flyway` and
`airflow-init` are completed one-shots, `oauth2-proxy` is the documented
distroless exception. The poller must treat "no healthcheck configured" as
*not pollable* rather than as *not healthy*, and it should read that exemption
from one place shared with `TestServiceHealthCoverage` rather than keeping a
second copy of the list.

### 3. Intent is released from an `EXIT` trap, including on failure

```bash
trap _on_exit EXIT
```

This is **probably correct and definitely undocumented.** Releasing deploy intent
when a build fails is right: nothing was recreated, so blocking DAGs serves no
purpose, and a Telegram alert fires. But the reasoning exists nowhere, and the
failure mode it does *not* cover is a partial recreation — `up -d` failing
halfway leaves a mixed fleet with intent released and work resuming.

Decide the behaviour deliberately, write it down, and distinguish the two cases:
build failure (safe to release) from recreation failure (not obviously safe).

### 4. The script cannot deploy a config-only change at all

Found during [Plan 140](plan_140_service_health_contract.md) Stage 2's
production deploy, 2026-08-20. `redeploy.sh` only knows `build` + `up -d`, and
neither applies a change to a **bind-mounted config file** — Compose sees no
service-config drift and correctly does nothing.

Worse than "does nothing": several services mount a **single file** rather than
a directory — `prometheus/prometheus.yml`, `promtail/promtail.yml`,
`loki/loki.yml`, `grafana/statsd_mapping.yml`. A single-file bind mount pins the
**inode**, and `git pull` replaces the file instead of editing it in place. The
container therefore keeps reading the old, now-unlinked file. A config *reload*
does not help and is actively misleading:

```
docker kill -s HUP cartracker-prometheus
# → level=INFO msg="Completed loading of configuration file"   ...the OLD one
```

Diagnose in two lines; different inodes mean the container is reading a deleted
file:

```bash
stat -c %i prometheus/prometheus.yml
docker exec cartracker-prometheus stat -c %i /etc/prometheus/prometheus.yml
```

**`docker restart` is the fix** — Docker re-resolves bind mounts at container
start, so a restart picks up the new inode and a recreate is unnecessary.
Directory mounts (`grafana/provisioning`, `grafana/dashboards`) are immune,
because names resolve per access.

This is the same class as defects 1-3: the script reports success from an action
that did not do what the operator believed. It should grow a config-deploy path
that restarts the affected services and then **verifies the loaded config**,
rather than leaving the operator to know which mounts are files and which are
directories. Note that Grafana's *alerting* provisioning is read only at
startup, while its dashboard provider re-reads every 30s — so "restart Grafana"
is required for rules and optional for dashboards.

### 5. A recreate silently orphans peers that cached the old address

Found 2026-08-20 by a human noticing empty dashboard panels, and owed to this
plan by [Plan 136](plan_136_solver_recycle_and_liveness.md) D6 item 3: *"a
deploy-time check for the class, not this instance."*

The Plan 140 Stage 1 deploy recreated `statsd-exporter` on 2026-08-18 at
17:03:06, which gave it a new IP. Airflow's Python StatsD client resolves its
destination **once, at construction**, then `sendto()`s the cached address. The
scheduler had been up since 04:58 with `restarts=0`, so for **two days and four
hours** it addressed UDP packets at an IP nothing was listening on. UDP fails
silently: no exception, no log line, no error metric. Seven of eight Pipeline
Health panels went blank and `ct-pipeline-failures` had no input at all, which
`noDataState: OK` rendered as a quiet green rule.

`airflow_ti_successes` survived and is the diagnosis: task metrics come from
short-lived LocalExecutor processes that resolve DNS fresh each run. **The split
is by process lifetime, not by metric** — so that metric looking healthy is not
evidence of anything.

Nothing in the stack could see it. The exporter was healthy and *is* healthy;
`up{job="airflow"}` was 1 throughout, because that job scrapes the exporter
rather than the scheduler. This is defect 4 with the transport swapped: an
action that reports success while doing something other than what the operator
believed.

**`promtail` and `postgres-exporter` were audited and are not exposed.** The
hazard is connectionless transport, not exporters. `promtail` pushes to `loki`
over HTTP and `postgres-exporter` dials `postgres` over TCP; both see a
connection error when the peer is recreated, re-resolve, and recover, and both
surface the failure while it lasts. Every Prometheus scrape target is the same:
`up` goes to 0. `statsd-exporter` is the fleet's only UDP receiver, and `up`
cannot see a dead UDP pipe by construction.

### 6. A recreate that recreated nothing reported success

Found 2026-08-20, hours after defects 1-5 shipped, while looking for the right
way to restart the Airflow processes from defect 5. Dry-run against production:

```
$ docker compose up -d --no-deps --dry-run \
      airflow-dag-processor airflow-triggerer airflow-apiserver
DRY-RUN MODE -  Container cartracker-airflow-apiserver      Running
DRY-RUN MODE -  Container cartracker-airflow-dag-processor  Running
DRY-RUN MODE -  Container cartracker-airflow-triggerer      Running
```

No image change and no service-config drift, so `up -d` leaves the containers
running and exits 0. That is correct Compose behaviour and it is
indistinguishable, in the script's output, from a deploy that worked: the
default path would have built three ARM64 images, changed nothing, and printed
*"Done — every pollable service reported healthy."*

**This is defect 4's shape inside the fix for defect 4** — success reported for
an action that did nothing. Container ids are now sampled before `up -d` and
compared after; unchanged services are named, with a pointer to `--restart`.

## Out of scope

- **Replacing the deploy mechanism.** [Plan 88](PLANS.md) (Kubernetes) is the
  plan that would make this script unnecessary, and its trigger has not fired.
  This plan makes the current mechanism honest; it does not defend it forever.
- **Deploy intent semantics.** The `/deploy/start` and `/deploy/complete`
  endpoints and the drain contract are unchanged here.
- **Whole-host maintenance.** Plan 142 owns the reboot/apt procedure and should
  consume this script rather than fork it.
- **A deploy trigger endpoint.** That is [Plan 108](plan_108_deploy_trigger_endpoint.md),
  which is backlogged pending Plan 136's narrower restart-authority design.
- **Airflow's orphaned metrics.** Plan 136 owns that and already fixed it with a
  restart. Defect 5 here is only the deploy-time warning for the *class*.
- **Pipeline Health dashboard rot.** Panels 3 and 6 query metric names Airflow
  stopped emitting at the 3.x migration, and `grafana/statsd_mapping.yml` still
  maps `airflow.dagrun.schedule_delay.*`. That is Plan 141's dashboard contract.

## Decisions

Each defect asked for a judgement, not just a patch. These are the five, and
they are also written into `scripts/redeploy.sh`'s header, where an operator
reading the script at 2am will actually find them.

### Dependencies are checked, never recreated — and the check is not a gate

`--no-deps` is added. The remaining question was whether the script should
*verify* dependency health in its place. It does, but only on failure: a health
timeout prints `docker compose ps`, so the operator sees the fleet state at the
moment the deploy broke.

It is deliberately **not** a pre-flight gate, for two reasons. Healthchecks in
this system are shallow by contract — `test_probes_never_reach_across_to_another_container`
forbids a probe from touching another container — so a target reporting healthy
proves nothing about its dependencies, and a gate would be claiming a guarantee
it cannot make. And refusing to deploy while an unrelated service is unhealthy
blocks shipping the fix during the incident that needs it.

### The health timeout is derived from the compose file, not chosen

Docker can take `start_period + retries * (interval + timeout)` to settle a
container's health. Across `docker-compose.yml` the worst case is
`30 + 5 * (30 + 10)` = **230s**, from the Plan 140 Stage 1 convention that
twenty-four services share. The default is **300s**, and
`DEPLOY_HEALTH_TIMEOUT` overrides it.

The 70s of headroom is not the interesting part. The interesting part is that
`test_the_health_timeout_covers_the_slowest_healthcheck` recomputes the worst
case from `docker-compose.yml` on every CI run, so raising a `start_period` past
the deploy timeout fails CI instead of manufacturing a deploy failure months
later. The number stops being a guess the moment it is checked against its
source.

### Intent is released on a failed build and **held** after a failed mutation

The old `trap _on_exit EXIT` was right about build failures and silent about
everything else. The two cases are now split on one variable, `MUTATED`:

- **Nothing was recreated** — bad arguments, a failed build, a missing
  exemptions file: **release**. No container changed, so blocking DAGs serves no
  purpose, and a Telegram alert fires.
- **A container was recreated or restarted and something then failed**:
  **hold**. `up -d` failing halfway leaves a mixed fleet, and resuming work
  against one is worse than a stalled pipeline.

Held intent is loud, not stuck. `deploy_intent_sensor` polls for 600s and then
fails the DAG run, which pages — the escalation `shared/deploy_intent.py`
deliberately designs for ("an intent nobody cleared is a real problem and not
one a long job should paper over by starting anyway"). It also does not wedge
the *next* deploy: `/deploy/start` steals a lock older than `STALE_LOCK_MINUTES`
= 30. The Telegram alert names the phase and says which way intent went.

### The single-file bind mount goes in all three places, and each does one job

Defect 4 asked whether the fix belonged in the script, in a procedure, or in a
test. It belongs in all three, because they answer different questions, and
picking only one leaves a hole:

| Where | What it does | Why not the others |
|---|---|---|
| `redeploy.sh --restart` | Restarts the service and then **verifies** by comparing host and container inode | A procedure cannot verify; a test cannot deploy |
| The script header + this doc | Records why a restart and not a `SIGHUP` reload | The script alone does not explain why the obvious thing is wrong |
| `TestSingleFileBindMounts` | Fails when a **seventh** single-file mount appears | Neither of the others fires until someone already deployed it wrong |

The test cannot forbid single-file mounts — six exist (`prometheus.yml`,
`promtail.yml`, `loki.yml`, `statsd_mapping.yml`, `Caddyfile`,
`oauth2-proxy.cfg`) and all six are reasonable; the plan's original list of four
missed the last two. What it can do is make adding a seventh deliberate, which
moves the trap out of an operator's memory and into CI. That is the whole point:
the Plan 136 Stage 2 deploy caught this a second time *only because the earlier
finding said to look*.

Verification is by inode rather than by content, because the container is
reading an unlinked file whose content may well be identical to some past
version. `stat -c %i` on both sides answers "is this the file that is on disk
now?" directly. Where the image has no usable `stat` — `oauth2-proxy` is
distroless — the script reports `UNVERIFIED` and says so in its closing line
rather than passing quietly.

### A recreate warns about cached-address peers; it does not restart them

`deploy-followers.txt` names services whose peers resolve them once and cache
the result, and the script prints the entry verbatim after a recreate.

It does not restart them. Bouncing the Airflow scheduler as a side effect of
deploying `statsd-exporter` is precisely the blast-radius defect `--no-deps`
exists to stop, and the follower may need its own drain. A deploy tool whose
effects exceed its argument list is the thing this plan is fixing, so the script
warns — loudly, with the exact command — and the operator acts.

The registry is prose, not a machine-readable follower list, because its only
consumer is a human reading a terminal. It is checked all the same:
`TestCachedPeerAddressRegistry` asserts that every named service exists, that
`statsd-exporter` is registered with a `docker restart` command, and that all
four long-lived Airflow processes are named — not just the scheduler, which is
merely the one that was noticed.

### The mode is named for its mechanism, not for its first use case

It shipped as `--config`, named after the bind-mounted config file that
motivated it. The second use case arrived the same day and is not a config
change: three Airflow processes holding a dead `statsd-exporter` address need
their *process* restarted, with the image and service config untouched.

The mechanism is restart-and-verify, so the mode is `--restart`. `--config`
remains an accepted spelling, because "deploy this config change" is still the
most common reason to reach for it and reads better at the call site than the
mechanism does. One mode, two honest names; a test asserts both are accepted.

The general rule, now in the script's usage text: **pick the mode by what has to
change.** New code is the default path. A process that must restart while its
image and config stay put is `--restart`. There are exactly two known reasons
for the latter, and they are defects 4 and 5.

### `--restart` deliberately does not consult the follower registry

`docker compose restart`
reuses the container and its address, so no peer is orphaned; a test pins that
reasoning to the code, so if config mode ever starts recreating, the warning has
to move with it.

## Success criteria

| Metric | Gate |
|--------|------|
| Deploying a service list | Recreates exactly that list; no dependency is recreated as a side effect |
| Deploy completion | Returns only after every recreated, pollable service reports `healthy` |
| Deploy timeout | Fails loudly and alerts; never reports "Done." on an unhealthy fleet |
| Deny-list exemptions | Read from one shared source, not a second hand-maintained copy |
| The stale TODO | Gone, with the intent-release behaviour documented in its place |
| A bind-mounted config change | Applied and **verified loaded**, not merely reloaded; single-file mounts restart rather than reload |
| Recreating a cached-address peer | Names the senders that must be restarted, and does not restart them itself |
| A recreate Compose declined to make | Reported as a no-op, never as a successful deploy |

## Verification

### Done — every path rehearsed against a stubbed Docker

The script's own branches were exercised on 2026-08-20 with `docker` and `curl`
stubbed on `PATH`, because the interesting paths are the failing ones and none
of them should first run on production:

| Path | Result |
|---|---|
| Happy path, one service slow to go healthy | Waited 13s for it, then `Done.`; intent released |
| Health timeout | Failed at the deadline, dumped `docker compose ps` and the healthcheck log, **held** intent, alerted |
| Container `exited` | Failed immediately rather than waiting out the timeout |
| Failed build | **Released** intent and alerted — the deliberate other half of the rule |
| Exempt service in the list | `flyway` skipped as exempt, not waited on |
| Unexempt service with no health status | Warned and continued; not treated as unhealthy |
| `--restart`, inodes match | `OK ... inode N matches the file on disk` |
| `--restart`, container reading a deleted inode | `STALE`, non-zero exit, intent held |
| `--restart`, no usable `stat` in the image | `UNVERIFIED`, reported in the closing line, exit 0 |
| Recreating `statsd-exporter` | Printed the follow-up note with the four containers to restart |

The three load-bearing assertions were also mutation-checked: removing
`--no-deps`, restoring `sleep 10`, and raising a `start_period` to 600s each
fail the suite.

### Done — read-only production sweep, 2026-08-20

Run after merging PR #224 and pulling to `1b66213` on the VM. The pull was
inert: the incoming set was docs, tests, `scripts/` and two new root data files,
with no `airflow/dags`, no `docker-compose.yml` and no bind-mounted config, so
no running container reads any of it. Nothing was restarted or deployed.

| Assumption | Result |
|---|---|
| `docker compose ps -q <svc>` resolves a container | Works on Compose 2.40.3, including the profile-gated `trawl` and `redis-trawl` **without** `--profile` — a concern raised during review and now closed |
| The health format string returns `<status> <health>` | Correct across all 28 running services |
| Exempt services report no health | `oauth2-proxy` → `none`; `flyway` and `airflow-init` have no container at all. All three are exempt, and exemption is checked *before* container resolution, so none reaches the "no container after deploy" error |
| The exemptions parser | Yields exactly the six expected names under the VM's bash 5.1.16 |
| `bash -n scripts/redeploy.sh` | Parses |
| Inode verification on all six single-file mounts | `prometheus`, `loki`, `promtail`, `statsd-exporter`, `caddy` all `OK`; `oauth2-proxy` `UNVERIFIED`, as designed |
| Deploy intent | `none` — nothing stuck |

`prometheus` reads inode **519823 on both sides**. That is the *host* inode from
the Plan 136 Stage 2 finding, where the container was pinned at 519700 — so that
drift has since been cleared by a restart, confirming the mechanism from both
directions.

### The sweep also found defect 5 live, and the D6 fix incomplete

Container start times against `statsd-exporter`'s recreate at 2026-08-18
17:03:06:

| Container | Started | Predates the recreate |
|---|---|---|
| `airflow-scheduler` | 2026-08-20 21:11:53 | no — restarted by D6 |
| `airflow-apiserver` | 2026-08-18 15:39:41 | **yes** |
| `airflow-dag-processor` | 2026-08-18 04:58:36 | **yes** |
| `airflow-triggerer` | 2026-08-18 04:24:33 | **yes** |

An empty query result could mean "renamed at the Airflow 3.x migration" rather
than "sender orphaned" — the trap the Pipeline Health panel rot sits in — so
each metric was point-queried *before* the recreate as a known-good control:

| Metric | Before 17:03:06 | Now |
|---|---|---|
| `airflow_dag_processing_processes` | 3,416,623 | **empty** |
| `airflow_triggers_running` | present (0) | **absent** — empty vector, not 0 |
| `airflow_scheduler_heartbeat` | 1,539,610 | 1,252 — **live again** after its restart |

They were being ingested and stopped, so this is not a naming change.
`airflow-dag-processor` and `airflow-triggerer` have been sending UDP into the
void for over two days and still are; `airflow-apiserver` matches by start time
though no metric isolates it. `up{job="airflow"}` reads **1** throughout.

This validates the registry rather than contradicting it: `deploy-followers.txt`
already names all four on the reasoning that the scheduler was merely the one
that was noticed, and now that has production evidence.

**Not urgent, and deliberately not fixed here.** No alert rule references any of
the dead metrics — the only Airflow metric in `rules.yml` is
`airflow_dagrun_duration_failed_count`, feeding `ct-pipeline-failures`, emitted
by the scheduler, which is already restored. What is missing is dashboard
telemetry. The fix is
`redeploy.sh --restart airflow-dag-processor airflow-triggerer airflow-apiserver`
after the soaks close; the scheduler is excluded because restarting it again is
pointless churn.

### Owed — one low-risk production deploy

Not started; nothing here has run on the VM. `pgadmin` is the candidate: it is
on no critical path, and it was one of the two services observed still
`starting` past the old ten-second mark. Confirm that it waits for real health,
that no dependency is recreated, and that intent is released. Then
`--restart prometheus`, which is the mount that produced defect 4 twice.

Both must wait for the Plan 136 Stage 2 and Plan 140 Stage 2 soaks to close
(2026-08-21, ~20:42 and ~19:04 UTC). A deploy during either window disturbs the
evidence they exist to collect — Plan 140 Stage 2's soak is specifically
counting container-health alert instances, and recreating containers is what
produced its first false page.
