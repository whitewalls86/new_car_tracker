# Plan 142: Scoped Operational Coordination and Host Maintenance

## Status

DRAFT, written 2026-08-18 after the first deliberate whole-host maintenance
window exposed that the repository has a deploy procedure and a storage
runbook, but no durable procedure for pausing production, updating Ubuntu,
rebooting the VM, proving the host and stack healthy, and safely resuming work.

**Reframed 2026-08-25:** Stage 1 now replaces deploy intent with one scoped
operational-coordination contract rather than adding a second state machine.
A deploy is a short maintenance window over selected operational surfaces; a
host window selects the whole host and additionally permits an offline phase.
The existing `/deploy/*` API remains as a compatibility facade during rollout.

Priority **86 (high)**. Effort **M plus the first observed maintenance window**.

This plan is separate from [Plan 140](plan_140_service_health_contract.md).
Plan 140 produces the trustworthy post-restart health signal; Plan 142 owns the
state machine and operator workflow around a host outage.

## The production evidence

The 2026-08-17/18 Plan 135 deployment included overdue Ubuntu maintenance
because both required a planned production pause. It completed successfully,
but only through live investigation and manual coordination:

- `apt-daily` had been stuck for **66 days**. It had to be stopped carefully,
  followed by a clean `dpkg --audit` and package-index refresh. Interrupting an
  active `dpkg` transaction would have been materially riskier.
- Restoring the apt timers allowed `unattended-upgrades` to start during the
  controlled window. It was allowed to finish, but the package authority was no
  longer entirely in the operator's hands.
- The ordinary deploy-intent sensor has a **10-minute timeout**. The maintenance
  exceeded it, so two `check_deploy_intent` tasks failed before doing work.
  That is correct for a short deploy and wrong for a host-maintenance window.
- `/deploy/status.number_running` combined pending artifacts with genuinely
  processing work. Twenty-two durable pending rows looked like active work even
  though there were zero processing artifacts and zero running detail claims.
- The default Compose project did not describe all runtime intent. `trawl` and
  `redis-trawl` are profile-gated, while MLflow and Lakekeeper use separate
  Compose projects and were intentionally stopped for Plan 125. A generic
  `docker compose down` cannot distinguish “forgotten” from “deliberately
  paused.”
- ~~The Docker package upgrade raised the daemon's minimum API version and broke
  Promtail 2.9.8 discovery. The host update therefore changed an application
  compatibility boundary even though no application code caused it.~~
  **Disproved 2026-08-23 by `/var/log/apt/history.log` — there was no Docker
  upgrade, in this window or ever.** `docker.io` was installed once, already at
  29.1.3, on 2026-05-19 20:20 by a hand-run
  `apt-get install -y docker.io docker-compose-v2 git` during the Plan 105 VM
  build. None of the 98 `unattended-upgrade` transactions on this host has ever
  touched it. The daemon's minimum API version has therefore been 1.44 since the
  host existed, and the Promtail 2.9.8 incompatibility was **latent from day
  one**. What changed in the window was Plan 135 Stage 5 making Promtail do
  Docker service discovery for the first time — an *application* change
  revealing a pre-existing boundary, which is the exact opposite of the reading
  above. The corrected lesson is that a maintenance window surfaces latent
  incompatibilities because it is the first time in months that everything is
  recreated at once, and that argues for the same post-restore verification
  rather than for pre-install package review.
- Recovery required checking the running kernel, `/` and `/mnt/data` mounts,
  Docker configuration, selected services, Loki/Promtail ingestion, and the
  intentionally stopped Plan 125 services before work could safely resume.

The outage was successful, but the procedure lived in the conversation. The
next maintenance window should be an execution of checked-in policy rather than
a reconstruction of it.

## D9 — Deploy and maintenance are one coordination problem, 2026-08-25

The first Stage 1 implementation started from the plan's original instruction:
create `maintenance_state` beside `deploy_intent` and make the two mutually
exclusive. Before that code was committed, the drain design reproduced the
reason the existing deploy sensor failed.

Deploy intent protects a short service replacement:

- expected duration under ten minutes;
- most of the host stays available;
- the ops API can release intent;
- an exit trap releasing intent is usually safer than leaving it stuck;
- a targeted `docker compose up -d SERVICE` is the normal mutation.

Host maintenance adds stronger properties:

- package work and reboot duration are variable;
- Postgres, Airflow, ops, and the intent API are deliberately offline;
- the kernel, mounts, network, Docker daemon, and every Compose project may
  change state;
- automatic release on timeout or script exit could resume work onto a broken
  host;
- rollback may require an older kernel or package.

Those differences require different scopes and release gates, **not two intent
systems**. Both operations need to stop admitting affected work, drain work
already admitted, mutate a bounded target, validate it, and release explicitly.
The old global sensor instead blocks every DAG, times out at 600 seconds, and
counts durable pending artifacts as running work. Building a second mechanism
would preserve those defects for every ordinary deploy.

Stage 1 therefore builds one coordination record with:

- `kind`: `deploy`, `service_maintenance`, or `host_maintenance`;
- `scope`: named operational surfaces and the checked-in service targets that
  selected them;
- `phase`: `none`, `requested`, `draining`, `active`, `validating`;
- requester, reason, timestamps, expected work, manifest/checkpoint location,
  and operator notes.

`active` means “the requested mutation is authorized.” Only
`host_maintenance` may take Postgres/the host offline. The host checkpoint is
still required while Postgres is unavailable, and Postgres is authoritative
again when it returns.

The existing `/deploy/start`, `/deploy/status`, and `/deploy/complete` routes
remain during migration, translating their callers into scoped coordination
operations. DAGs and cooperative long jobs move to the new contract before the
legacy table or sensor is removed. There is no flag day on the production gate.

## Goals

1. Replace deploy intent and host-maintenance intent with one durable, scoped
   coordination contract and a compatibility path for existing callers.
2. Stop only affected new production work without turning an expected pause
   into failed DAGs.
3. Drain active work using counts that distinguish queued backlog from work that
   is actually mutating state.
4. Preserve the intended running/stopped state across a whole-stack stop and
   host reboot, including profile-gated and auxiliary Compose projects.
5. Make targeted service work and Ubuntu package/reboot work normal,
   reviewable operations using the same safety contract.
6. Refuse to resume an affected surface until its scoped health gates pass.
7. Keep coordination active after any ambiguous failure; release is always an
   explicit operator action.
8. Capture enough before/after evidence to diagnose or roll back the change.

## Non-goals

- Zero-downtime host patching. This is a single production VM and the window is
  intentionally user-visible.
- Automatic package installation or unattended reboot.
- Giving an application container unrestricted host or Docker authority.
- Replacing `scripts/redeploy.sh`; it becomes a client of the coordination API
  and keeps owning build/recreate/restart mechanics.
- Defining service healthchecks. Plan 140 owns them.
- Solver-only recycling and its automatic restart authority. Plan 136 owns it.
- Provisioning a second host or orchestrator. Plans 69 and 88 cover those
  architectural changes if they become justified.

## Design principles

### Safety biases toward staying paused

A forgotten maintenance state is inconvenient and alertable. Resuming DAGs on
an unverified host can corrupt work or amplify an outage. Maintenance must not
auto-release because a lease expired, the shell disconnected, the VM rebooted,
or an `EXIT` trap ran.

The state persists in Postgres and survives application-container replacement.
While Postgres is offline it is intentionally unreadable; the host-side script
also records a small non-secret checkpoint under a dedicated state directory so
an operator can tell which phase was in progress from the console. The database
remains authoritative once it is back.

### Observation and mutation are separate phases

Package refresh, compatibility review, image pulls, disk checks, and Compose
rendering happen before production pauses.
Only draining, stopping, installation, reboot, validation, and resume consume
the window.

### The running-set manifest is evidence

Before stopping anything, inventory containers using Compose labels:

- project name;
- working directory and config files;
- service and enabled profiles;
- image ID/digest;
- running, stopped, and health state;
- restart and Docker log-rotation configuration.

The manifest explicitly records services intentionally stopped before the
window. Recovery compares against that manifest; it does not blindly start
every Compose file found on disk. Plan 125's MLflow/Lakekeeper pause is the
first required fixture for this behavior.

### Scope is an operational surface, not merely a container name

The operator names services or `host`; a checked-in registry expands those
targets into surfaces and known followers/dependencies. Initial surfaces are
`detail_fetch`, `processing`, `archive`, `analytics`, `airflow_control`,
`observability`, `ingress`, `database`, and `host`. Recreating Grafana need not
pause scraping; replacing Postgres selects every Postgres-dependent surface.
The registry also carries observed indirect effects such as a recreated
`statsd-exporter` requiring Airflow processes that cached its address to
restart. Scope expansion is printed and reviewed before intent is requested.

Each mutating DAG entry point declares the surfaces into which it admits work.
A rescheduling gate blocks only when those declarations intersect the active
scope. It has no short timeout: stale coordination alerts independently, so a
planned pause never manufactures a failed DAG merely because time passed.

The service and surface identifiers are stable enough for other checked-in
contracts to reference. In particular, [Plan 139](plan_139_test_suite_maintenance.md)
may map changed paths and CI test/image groups onto them. CI selection remains
a separate graph with separate fail-closed evidence: this production registry
must not acquire path globs, test names, or skip policy, and it never proves by
itself that a CI job is safe to omit.

Service identity also carries an explicit execution lifecycle: continuously
expected service, profile-gated continuous service, initialization job, or
one-shot workload. This is descriptive safety policy, not launch machinery.
In particular, `pack-worker` remains continuous because that is how production
runs today, while `snapshot-worker` is one-shot. A stopped continuous service
and an absent idle one-shot workload are different states and cannot share a
health or drain interpretation. [Plan 152](plan_152_scheduled_worker_lifecycle.md)
owns changing pack and disk measurement to one-shot execution; when it lands,
Plan 142 changes the lifecycle declaration and evidence adapter without
changing the coordination state machine or operational surfaces.

## Coordination state machine

The exact schema is decided in Stage 1, but the externally visible states are:

| State | Meaning | Allowed transition |
|---|---|---|
| `none` | Normal scheduling and claims | `requested` |
| `requested` | Intent is durable; its scope is fixed | `draining`, `none` (cancel) |
| `draining` | New in-scope work is gated; admitted work drains | `active`, `none` (cancel) |
| `active` | Scoped mutation is authorized; host kind may be offline | `validating`, `none` only for an unchanged/aborted target |
| `validating` | Target is back, but its surfaces remain gated | `none` only after scoped release gates pass |

There is one active coordination row, so deploy and maintenance cannot race or
coexist. No stale-lock takeover silently converts one kind into another.

The coordination record includes requester, reason, requested/start timestamps,
phase timestamps, expected work, running-set manifest location, and operator
notes. A stale-state alert pages an operator but does not release the state.

That record holds the *current* window only. Its transition history is durable
separately, in `staging.coordination_state_events` — see Stage 2's
[durable transition history](#durable-transition-history).

## The drain contract

`number_running` is replaced by named counts filtered to the active scope:

- artifacts with `status='processing'`;
- detail claims with `status='running'`;
- active Airflow task instances that can mutate production;
- long-running pack/prune/maintenance endpoints still inside a non-interruptible
  section;
- any service-specific claim introduced later.

Pending artifacts are durable backlog and **do not block a stop**. Each count
has its own oldest-start timestamp so one stuck claim is identifiable.

The **admission set** and **drain set** are different and both are checked in.
For `scrape_detail_pages`, `claim_batch` is the admission gate, while an already
admitted `scrape_detail`, its external scraper batch, `release_claims`, running
detail claims, and artifacts already in `processing` belong to the drain set.
Holding processing must never make a durable pending artifact block forever.
An unreadable count, missing gate, or unreachable external job registry is
`unknown`, never zero.

Trawl does not contribute a separate Redis drain count. Its Redis state is a
session cache, not admitted job backlog, and its scrape request is synchronous
inside the scraper detail job already counted above. Treating cached sessions
as running work would manufacture the same pending-versus-active error this
plan removes. Compose one-shot work is counted separately from live container
state through the existing read-only container-health boundary.

During `draining`, new in-scope DAG work reaches a coordination-aware
rescheduling gate that does not fail after ten minutes. Stage 0's pool remains
useful evidence for a whole-fleet hold and Plan 136's exclusive recycle, but a
task has only one pool and may touch several surfaces; it cannot express the
new scoped contract alone. The acceptance contract is:

- no new mutating task starts;
- already-running work reaches a safe boundary;
- an hour-long maintenance pause creates no failed DAG solely because of the
  pause;
- resuming does not unleash an unbounded duplicate backlog.

## Stages

### Stage 0 — Turn the successful window into fixtures and decisions

Documentation and tests only:

1. Record the 2026-08-17/18 timeline, commands, failure modes, intended-stopped
   services, and recovery evidence as a sanitized fixture/runbook example.
2. Inventory every current Compose project and profile from source and define
   the expected default running set plus justified stopped/exempt entries.
3. Prototype the Airflow maintenance gate and measure what scheduled runs do
   during a one-hour pause: queue, reschedule, coalesce, or backfill. Three
   mechanisms, not two — **an Airflow pool is the third and the one to beat**:

   | Mechanism | The cost to weigh |
   |---|---|
   | Dedicated sensor / state gate | A sensor occupies a worker slot for the length of the pause, and it is the shape whose 600-second failure this plan exists to avoid |
   | DAG pause/unpause with a manifest | Requires recording the prior pause state and restoring it. A maintenance run that dies halfway leaves DAGs paused with **no record of which were already paused** — the worst failure mode on this list, because it is silent and the fleet looks fine |
   | **Pool: every mutating task takes 1 slot, the maintenance hold takes all** | Pooled tasks **queue rather than fail**, which satisfies the acceptance contract directly. No manifest exists to lose: a hold task that dies releases its slots and normal scheduling resumes on its own. Measure what the queued backlog does on release |

   **A pool gates, it does not count.** It cannot distinguish durable pending
   backlog from a running claim, which is the distinction the drain contract
   turns on, and it is blind to work this scheduler did not start — `ops` HTTP
   endpoints and long-running pack/prune sections among them. It replaces one
   mechanism inside the contract, never the contract. Prototype it against the
   named counts, not instead of them.
4. Define package classes: security updates, ordinary updates, Docker/container
   runtime, kernel, and packages requiring service restart.
5. Choose the host checkpoint directory and permissions; it contains phase and
   revision metadata only, never secrets.
6. Rotate `AIRFLOW_JWT_SECRET` to 64 random bytes during the Airflow restart
   item 3 already budgets. **Inherited 2026-08-23** from Plans 141 and 136,
   which both routed it to Plan 136 Stage 0 *after* that stage closed on
   2026-08-18; it was owned by nobody until this line existed.
   `AIRFLOW__API_AUTH__JWT_SECRET` ([docker-compose.yml](../../docker-compose.yml),
   `x-airflow-common`) is 35 bytes, under the 64 RFC 7518 §3.2 recommends for
   HMAC-SHA512, so PyJWT emits `InsecureKeyLengthWarning` on every apiserver
   start — 11 times since 2026-08-21, in the exact stream Plan 141 is trying to
   make worth reading. `AIRFLOW__CORE__FERNET_KEY` is 44 bytes and is not
   involved.

   **Hygiene, not a vulnerability, and the difference was measured rather than
   assumed.** Sampled on the apiserver 2026-08-23 without printing the value:
   35 characters, 25 distinct, mixed case and digits, ~4.5 bits of Shannon
   entropy per character. That is a generated string, not a passphrase, so the
   key material is far past brute force — the RFC's rule compares key length to
   hash output and says nothing about a break. It rides this window because
   rotation invalidates in-flight worker tokens, not because it is urgent. It
   is also a fair first exercise of what this plan is building: a low-risk
   change that wants exactly the drain-and-restart discipline Stage 1 defines.
7. Give `caddy` a `restart: unless-stopped` policy, in the same window.
   **Moved here from Stage 2 on 2026-08-23**, because Stage 2 is weeks away and
   the gap is live: an unplanned reboot takes the public site down and, per
   finding 2 below, publishes no signal that it did.

   Two mechanics this must not get wrong, both already learned on this host:

   - It is a **service config change, so it needs `docker compose up -d caddy`,
     which recreates the container.** `docker compose restart` reuses the
     existing container's config and would leave the policy silently unapplied
     — the exact trap Plan 135 hit on `node-exporter`, where the container came
     back looking healthy with the old flags still in place.
   - **Verify the policy, not the uptime.** `docker inspect --format
     '{{.HostConfig.RestartPolicy.Name}}' caddy` must return `unless-stopped`.
     Plan 135's rule was "always check `.Args`, not container uptime"; this is
     the same rule against a different field.

   Recreating `caddy` briefly drops `:80`/`:443`, so it is user-visible for a
   few seconds and belongs in a window rather than in a quiet moment. Its TLS
   material lives in the `caddy_data` volume and is unaffected. Nothing
   resolves `caddy` by name — it is the ingress, not an upstream — so
   [deploy-followers.txt](../../deploy-followers.txt) has no entry to honour
   here; the caching hazard runs the other way, from `caddy` to its upstreams,
   and those are not being recreated.

   When it lands, **delete `caddy`'s `restart-gap` entry from
   [`maintenance-running-set.txt`](../../maintenance-running-set.txt) in the
   same commit.** `tests/test_maintenance_running_set.py` asserts both
   directions, so a fix without the deletion fails CI rather than leaving the
   registry claiming a defect that no longer exists.

   **Built 2026-08-24**, both halves in one commit — see
   [items 6 and 7](#items-6-and-7-built-2026-08-24--and-only-one-of-them-is-a-commit).

Items 6 and 7 both ride item 3's Airflow window: it is the only production
touch Stage 0 makes, and neither change justifies a window of its own.

#### Item 3 Phase A built, 2026-08-23 — the pool assignment, shipped inert

**The pool wins, and the prototype splits in two.** A pool is a task attribute,
so prototyping it means putting `pool=` on tasks in DAG code — and that change
is *inert while the pool has free slots*. Behaviour is identical until someone
shrinks it. So:

- **Phase A (done, no window):** create the pool generously sized, add `pool=`
  to the mutating tasks, deploy as a normal change, soak a day. Nothing changes.
- **Phase B (the window):** shrink to 0, observe, release, measure the drain.

Phase A is not throwaway — it is how Stage 1 would ship this anyway. Abort is
`git revert`; there is no production state to unwind.

[`airflow/dags/pools.py`](../../airflow/dags/pools.py) holds the constant and
the reasoning; [runbook §9](../runbooks/runbook_host_maintenance.md) holds the
operator commands and the preflight checks;
`tests/airflow/test_maintenance_pool.py` holds the contract.

Five tasks are pooled — `results_processing.process_batch`, `orphan_checker`'s
three janitorial endpoints, and `scrape_detail_pages.claim_batch`. Sensors are
not: a `reschedule`-mode sensor must not hold a slot while it waits, which is
the failure this mechanism exists to avoid.

**The held set is three DAGs, not two.** `ops.ops_detail_scrape_queue` (view,
`V040__detail_scrape_circuit_breaker`) keys on `last_detail_scraped_at`, which
only the processing service writes (`processing/writers/detail_writer.py:194`),
while `POST /scrape/claims/release` deletes the claim. Holding processing alone
therefore leaves the detail scraper re-claiming the same ~100 listings every 15
minutes — up to four redundant passes an hour against cars.com, through the
solver Plan 136 is nursing. `last_detail_scraped_at` **is** the circuit breaker;
holding its only writer without holding its producer disables it. Plan 147 fixes
that ownership; until it lands the two are held together, which is also more
faithful to "no new mutating task starts". `scrape_listings` stays running —
it advances a rotation and never reads processed state, so it cannot loop.

##### Question 1 answered: two pools, and the reason is mechanical

Stage 1 currently promises one pool shared with Plan 136 Stage 3d. **That
promise cannot be kept, and not as a matter of taste.** In Airflow a task's
`pool` is a single scalar string — a task belongs to exactly one pool — so one
shared pool works only if the two held sets are identical or disjoint.

They are neither in intent: Plan 142 holds all mutating work, Plan 136 holds
solver-consuming work, a subset. But **by task they are disjoint**, and that is
the part worth noticing:

| Plan | Tasks it holds |
|---|---|
| 142 `maintenance` | `results_processing.process_batch`, `orphan_checker.{expire_orphan_detail_claims,reap_stuck_processing,evict_delisted_cooldowns}`, `scrape_detail_pages.claim_batch` |
| 136 `solver` | `scrape_detail_pages.scrape_detail`, `scrape_listings.run_scrapes`, `recycle_solver.recycle` |

The DAGs overlap; the tasks do not. The gate points differ because the *reasons*
differ — 142 gates the claim so no listing is ever claimed, 136 gates the fetch
because that is what touches the solver. Sizing settles it too: 136's pool is
exactly 2 slots so a 2-slot recycle achieves mutual exclusion, while 142's is 16
so the assignment stays inert. One pool cannot be both 2 and 16.

**So: two pools, and the rule Stage 1 should carry instead is that a
maintenance hold is a multi-pool operation** — hold every gating pool, not one.
That generalises: if 142 ever needs `scrape_listings.run_scrapes` held, it
cannot take the task from `solver`, but it can set `solver` to 0 alongside
`maintenance`. A list of pools expresses any number of scopes; one pool
expresses one.

##### Question 2 answered: `pools set 0`, and the hold must not be declarative

Item 3's own table praises the pool because "a hold task that dies releases its
slots and normal scheduling resumes on its own." **That is right for Plan 136
and backwards for Plan 142**, whose first design principle is that maintenance
never auto-releases — not on a lease expiry, a dropped shell, an `EXIT` trap or
a reboot. So the hold is `airflow pools set maintenance 0`: a row in the Airflow
metadata DB that survives all of those. The cost is that it survives forgetting
too, which is the direction this plan chooses to fail in, and which is why a
"pool held longer than N minutes" alert is owed by Stage 1 item 5.

Two things were verified against `apache-airflow-core` 3.2.0 rather than
assumed, because the acceptance contract turns on them:

1. **A pool-starved task queues rather than failing.** It stays in `SCHEDULED` —
   the scheduler sees `open_slots <= 0` and skips it
   (`scheduler_job_runner.py:703`) — so it never reaches `QUEUED`, never gets a
   `queued_dttm`, and is therefore invisible to `_get_tis_stuck_in_queued`, the
   only thing that fails a task for waiting, at `[scheduler]
   task_queued_timeout` = 600s (`scheduler_job_runner.py:2472`). "An hour-long
   pause creates no failed DAG solely because of the pause" is satisfied by
   construction. The deploy-intent sensor's identical 600s budget is what failed
   two `check_deploy_intent` tasks in the August window.
2. **A missing pool is silent.** The scheduler logs `Tasks using non-existent
   pool 'maintenance' will not be scheduled` (`scheduler_job_runner.py:693`) and
   skips — no failure, no alert, the five tasks simply stop. Hence the deploy
   ordering rule (**create the pool before the code lands**) and a new preflight
   line in [runbook §2](../runbooks/runbook_host_maintenance.md).

**And the pool is deliberately not created from git.** `airflow pools set` is an
upsert, so a declarative create in `airflow-init` — the obvious way to keep it
out of an operator's hands — would reset the slot count on every
`docker compose up -d`. The slot count *is* the hold state, and a maintenance
window recreates the stack, so that would silently release the hold mid-window.
This is design principle 1 defeated by a convenience. `tests/airflow/
test_maintenance_pool.py::TestTheHoldIsNotDeclarative` asserts Compose never
sets a pool. The price is item 2 above: the pool lives only in the metadata DB,
so a rebuilt DB loses it silently. Preflight is the mitigation until Stage 1 can
alert on it.

##### The pool exists, and the ordering rule was honoured

Created on production **2026-08-24 UTC, before the DAG code merged**, which is
the one-way ordering rule item 2 above establishes. Verified read-only from
`airflow pools list`:

    pool         | slots | description               | include_deferred
    =============+=======+===========================+=================
    default_pool | 128   | Default pool              | False
    maintenance  | 16    | Plan 142 maintenance gate | False

`include_deferred: False` is the default and is correct here — every pooled
task is a plain `PythonOperator` and none of them defer. It is worth knowing
the value only because Stage 1 adding a deferrable operator would make it a
decision rather than a default.

##### The Phase A soak gate

Phase A ships inert, which is the point and also the difficulty: an inert
change produces no signal that it worked, only the absence of signals that it
broke. So the gate is stated as things that must **not** happen, over at least
24 hours of ordinary operation after the merge lands on the VM.

Steady state is **28 pooled DAG runs an hour** — `results_processing` at `*/5`
(12), `orphan_checker` at `*/5` (12), `scrape_detail_pages` at `*/15` (4) —
against a peak concurrent demand of 5 slots out of 16.

| Check | Passing | What a failure would mean |
|---|---|---|
| DAG success rate for the three DAGs | Unchanged from the pre-merge day | The assignment is not inert after all |
| Scheduler log for `non-existent pool` | Absent | The pool was lost; all five tasks are silently unscheduled |
| `airflow pools list` | 16 slots, `used` at 0 or briefly low | Slots leaking, or an unreleased hold |
| Any pooled task sitting in `SCHEDULED` past its next schedule | None | The pool is binding when it should not be |

The soak has one honest weakness worth naming rather than discovering later:
**it cannot exercise the mechanism it is soaking.** Nothing here proves the
hold works — only that assigning the pool cost nothing. Phase B is the first
time `pools set 0` is ever run against production, and the acceptance contract
is measured there, not here.

##### Phase A soak record — closed clean 2026-08-25, called early at ~21.5h

Read at 01:50 UTC, about **21.5 hours** into the 24-hour window, and closed on
the readings rather than on the clock. The remaining ~2h15m is noted rather
than waited out. This follows the precedent of
[Plan 140's Stage 2 soak](plan_140_service_health_contract.md), closed the same
way at 21h28m of 24h with the reasoning recorded.

The argument for calling it: **"the assignment is inert" is a deterministic
property, not a rare stochastic one.** If `pool=maintenance` at 16 slots
against a peak concurrent demand of 5 were going to bind, it would have bound
in the first hour, not the twenty-third. There is no mechanism by which hours
22–24 reveal something hours 1–21 did not, and every gate below reads zero.

| Gate | Reading |
|---|---|
| Success rate vs the pre-merge day | `orphan_checker` **258 success / 0 failed**, against **258 / 0** in the prior 21.5h. `results_processing` **258 / 0** against **258 / 0**. `scrape_detail_pages` **86 / 0** against **86 / 0**. Identical on all three, in both directions |
| Scheduler log for `non-existent pool` | **0 occurrences** in 24h of logs |
| Slots held by the `maintenance` pool | **None.** No task instance in `running`, `queued` or `scheduled` carries the pool |
| Pooled tasks stuck in `SCHEDULED` | **None** past 15 minutes |
| Pooled task instances observed | **1,118** through the pool during the window |

602 DAG runs in 21.5 hours (258 + 258 + 86) against this section's documented
steady state of **28 pooled DAG runs an hour** — 21.5 × 28 = 602. The observed
load is exactly the load the gate was written for, which is the check that the
numbers above describe a normal day rather than a quiet one.

**What this does and does not unblock.** It releases the DAG re-serialization
constraint, which is what mattered on the day: Plan 140 Stage 4 changes
`airflow/dags/sensors.py`, every DAG imports it, and Phase A's 24 hours are
measured from re-serialization — so pulling before this gate closed would have
restarted the clock. It does **not** advance Phase B, which is gated on the
later of this and Plan 136's Stage 3a read at ~19:40 UTC.

##### Where Phase B picks up

**The window is written down.**
[Runbook §10](../runbooks/runbook_host_maintenance.md) is the whole approved
sequence end to end — preflight, caddy (item 7), the HMAC rotation (item 6), the
hold, the release and its measurements, restore — with the blast radius and
abort for each step. It is ~90 minutes, of which ~3 are user-visible, and it
touches no packages and no reboot.

Phase B is gated on **2026-08-25**, by two things landing the same day:

1. ~~**Phase A's 24-hour soak completes at 03:47 UTC**, measured from
   re-serialization rather than from the merge.~~ **Closed 2026-08-25**, called
   early at ~21.5h on the readings — see
   [the Phase A soak record](#phase-a-soak-record--closed-clean-2026-08-25-called-early-at-215h).
2. **Plan 136's Stage 3a memory baseline is read.** Phase B's hour-long hold
   stops detail scraping, which puts a trough in the very `trawl` memory curve
   Stage 3b sizes its recycle threshold against. SRP scraping continues, so the
   solver does not go idle and the perturbation is partial — but an unexplained
   trough in a curve someone is about to read a threshold off is the "gauge
   misread as truth" defect Plan 136's own D2 is about. Run Phase B after that
   read, or record the hold window in Plan 136 so the trough has an owner.

Item 7's commit must be **merged before the window opens**, because §10.2 starts
with `git pull` on the VM. It is a prerequisite, not a rider.

> **Corrected 2026-08-25 when the window ran: §10.2 no longer pulls.** Item 7's
> commit reached the VM's checkout in **PR #232**, several deploys earlier, so
> nothing needed pulling and pulling would have dragged in everything merged
> since. The prerequisite was real; the mechanism named for satisfying it was
> not. Runbook §10.2 now checks the key is present in the file instead.

##### Items 6 and 7 executed — 2026-08-25

Both ran after the evening deploy, in the same window, item 7 first as the
runbook prescribes.

**Item 7 — caddy's restart policy.** `docker compose up -d --no-deps caddy` at
19:58, verified by `docker inspect` reading **`unless-stopped`** where it had
read `no`. Site served throughout (`302`, the auth-proxy redirect for `/`).
Checked the *policy field*, never uptime — a running container proves nothing
about what happens after a reboot.

> **`--no-deps` was added to the runbook because of this run.** The bare
> `docker compose up -d caddy` the runbook then specified walked the dependency
> graph and re-ran `cartracker-airflow-init-1`, which is work nobody asked for
> inside a window whose entire purpose is a bounded blast radius.

**Item 6 — the HMAC rotation.** Old key **35 bytes**, new key **86**, past the
64 that RFC 7518 §3.2 recommends for HMAC-SHA512. Four services recreated with
`up -d --no-deps` — the value arrives through the environment, so a `restart`
would have kept the old one. All three verifications passed: `InsecureKeyLength`
count **0** in the apiserver log, scheduler heartbeat publishing (inside Plan
136 D6's 80-second expectation), and no DAG import errors. `.env` backed up to
`.env.bak-jwt-20260825T200738Z` before the edit.

> **One hazard worth naming for next time.** Copying the new value into a local
> `.env` produced a `.env.bak-jwt` holding the *old* secret, untracked **and not
> covered by `.gitignore`** — a live secret loose in a worktree another session
> may be editing. Deleted once verification passed. If you keep a local copy,
> confirm the ignore rule covers every backup name you create, not just `.env`.

##### Phase B run — 2026-08-25, hold 20:14:57 → 21:14:57 — ALL CRITERIA MET

> **Phase B has no observation window of its own, and never had one.** The
> "24-hour soak from 03:47 UTC" is **Phase A's** soak — a precondition for
> Phase B opening, struck through above and closed 2026-08-25 at ~21.5h. The
> Linear issue tracking Phase B restated that closed precondition as an open
> exit item, which cost real time on 2026-08-25 chasing a clock that was not
> running. Runbook §10's window is ~90 minutes end to end; when it closes,
> Phase B closes.

The hold ran a full 60 minutes and released cleanly. **Every acceptance
criterion passed, and the release answered the open question in a way that
removes a fix from the plan rather than adding one.**

**The hold.** Pool set to 0 slots at 20:14:57. Pooled tasks queued rather than
running, reaching 33 `orphan_checker` tasks and 11 live DAG runs by T+50m.
Throughout: `default_pool` completed 42 tasks normally and `scrape_listings`
succeeded, which is what proves the gate is **scoped rather than global**.

| Criterion | Result |
|---|---|
| No new mutating task starts | **0 detail artifacts** produced during the hold |
| No task fails merely because time passed | **0 failures** across the whole window |
| Unrelated DAGs unaffected | `default_pool` 42 success; `scrape_listings` success |

**The release.** Pool restored to 16 at 21:14:57.

| Measure | Result |
|---|---|
| Drain to zero | **74.5s** — first task 21:15:03.9, last 21:16:18.4 |
| Tasks released | 44 |
| `orphan_checker` | **39 tasks in 10.8s**, avg 1.7s each |
| `results_processing` / `scrape_detail_pages` | 3 tasks avg 2.2s / 2 tasks avg 1.2s |
| **Peak concurrency** | **16** |
| Failures | **0** |

##### The thundering herd is bounded by the pool, not by `max_active_runs`

**Peak concurrency was exactly 16 — the `maintenance` pool's slot count.**
`orphan_checker` held 11+ live DAG runs and 39 queued tasks and still could not
exceed 16 concurrent tasks, because a pool caps *tasks* regardless of how many
runs exist.

So the fix this section previously anticipated is **not required**. The earlier
text said "if it misbehaves, `max_active_runs=1` is the fix Stage 1 ships" — it
did not misbehave, and now we know why. **Adding `max_active_runs=1` to
`orphan_checker` on this evidence would constrain recovery throughput for no
safety gain**, and anyone proposing it later should be shown this measurement
first.

That is the argument for measuring before fixing, made concrete: pre-empting
would have shipped a constraint *and* left the pool's real role uncredited.
The honest qualifier is that the **dose** was larger than the question needed —
a 15-minute hold accumulates a quarter of the backlog and proves the same
mechanism. The hour came from sizing the window as a host-maintenance
rehearsal, not from what this measurement required.

##### The indefinite gate proved itself on a real run

```
scrape_detail_pages  scheduled__2026-08-25T20:15:00
  started 20:15:01  →  ended 21:16:10   (61 minutes, state=success)
```

**A DAG run sat admission-blocked for 61 minutes and then completed
successfully.** Under the 600-second global deploy sensor this replaces, it
would have failed at ten minutes — twice in August, per the evidence that
motivated Stage 1 item 4. This is that item demonstrated end to end rather than
argued.

**No re-scrape storm.** `scrape_detail_pages` fired exactly twice — the held
20:15 run and the next scheduled 21:15 run — each advancing state once, not
once per queued run. `max_active_runs=1` bounded it by construction, so the
four-runs-re-scraping-the-same-listings failure could not occur. 16
`price_observations` rows advanced after release; zero detail artifacts existed
during the hold.

##### The hold's signature in the trawl curve — it is not a trough

Phase B was gated on Plan 136 Stage 3a precisely so the hold's effect on the
`trawl` memory curve would have an owner rather than being read as solver
behaviour. Measured, the effect is real but **it is not the dip that was
anticipated**:

```
20:02    646.1 MiB     normal 15-minute detail batch spike
20:07    504.8 MiB
20:17    504.9 MiB   HOLD
20:37    517.7 MiB   HOLD
20:57    521.6 MiB   HOLD
21:12    541.1 MiB   HOLD
21:17    640.0 MiB     released batch
21:27    580.9 MiB
```

**The signature is the absence of the 15-minute batch spikes, not a trough.**
Each detail run normally drives memory to ~645 MiB and back; across the whole
hold there is not one, just a slow drift from 505 to 541 MiB.

**That drift is attributable and is not solver churn in the ambiguous sense.**
`scrape_listings` was deliberately never in the held set, so the solver kept
working every 30 minutes throughout. Absent spikes are the hold; the slow rise
is listing scrapes. The two causes separate cleanly, which is what this gate
asked for.

> **Stage 3b's pinned image did more than make the dip legible — it changed
> what the dip *is*.** On the old build, climbing +40.5 MiB/h from a 3+ GiB
> base, an hour of paused detail scraping would have shown as a mild flattening
> of a steep climb, and separating that from ordinary variance would have been
> guesswork. Against a bounded ~505 MiB baseline the missing spikes are
> unmistakable. CAR-9's blocker on Stage 3b was correct for a reason stronger
> than the one recorded at the time.

**A second mechanism, found in the same read.** `scrape_detail_pages` produced
runs at 19:00, 19:15, 19:30, 19:45, 20:00 and 20:15 — then **nothing at 20:30,
20:45 or 21:00** — resuming at 21:15. Only one run accumulated across the whole
hour, because `max_active_runs=1` kept the admission-blocked 20:15 run holding
the slot. The re-scrape storm was therefore **impossible by construction**, not
merely avoided: there was never a queue of runs to re-scrape the same listings.

> **One assumption carried, not verified.** "The endpoints are idempotent
> janitorial SQL, so the blast radius is load rather than corruption" was
> inherited from the runbook and relied on during the window. It was not
> independently checked, and the clean result does not confirm it — a herd
> capped at 16 may simply never have tested it. Anyone widening the pool or
> removing the cap should verify idempotency first rather than reading this run
> as evidence of it.

#### Items 6 and 7 built, 2026-08-24 — and only one of them is a commit

The two riders split cleanly by where their state lives, and that difference is
the whole reason they are handled differently.

**Item 7 is a commit.** `restart: unless-stopped` is on `caddy` in
[docker-compose.yml](../../docker-compose.yml), and `caddy`'s `restart-gap`
entry is deleted from
[maintenance-running-set.txt](../../maintenance-running-set.txt) in the same
commit — `test_caddy_restart_gap_is_recorded_while_it_exists` was written to
hold in both directions precisely so the deletion could not be forgotten, and it
now asserts the entry is gone. The `restart-gap` class itself stays defined
though it is empty: `caddy` was its only member, and a class that disappears
when it empties takes its reasoning with it, leaving the next policy-less
service nowhere to be classified.

Deploying it is [runbook §10.1](../runbooks/runbook_host_maintenance.md). It is
`docker compose up -d caddy`, never `restart`, and the check is
`docker inspect --format '{{.HostConfig.RestartPolicy.Name}}' caddy` rather than
container uptime.

**Item 6 is not a commit, and cannot be.** `AIRFLOW_JWT_SECRET` lives only in
the VM's `.env`, so what ships here is the procedure —
[runbook §10.2](../runbooks/runbook_host_maintenance.md) — with the generation
command, the `up -d` on the four Airflow services (not `restart`: the value
arrives through the environment, which a restarted container keeps), the three
verifications, and the abort. Nothing in git can assert the live key is 64
bytes; the `InsecureKeyLengthWarning` disappearing from the apiserver log is the
only evidence, and the runbook makes it the first check rather than an
afterthought.

##### A gap found while building item 6, and deliberately not fixed here

`.env.example` documents **none of the seven Airflow variables**
`docker-compose.yml` requires — `AIRFLOW_JWT_SECRET`, `AIRFLOW_FERNET_KEY`,
`AIRFLOW_DB_PASSWORD`, `AIRFLOW_APP_DB_PASSWORD`, `AIRFLOW_UID`,
`_AIRFLOW_WWW_USER_USERNAME`, `_AIRFLOW_WWW_USER_PASSWORD` (12 of the 42
variables Compose interpolates are missing in total). A fresh provision from
that template does not get a short JWT secret; it gets an **empty** one, which
is worse than the defect item 6 exists to fix.

Adding only `AIRFLOW_JWT_SECRET` was the tempting move and is the wrong one — it
would make the file look complete while six siblings stayed missing. This is a
provisioning concern rather than a rotation one, it is pre-existing, and it
wants either a full pass over the template or a test asserting every
interpolated variable is documented. Recorded here, owed to neither item 6 nor
this stage.

#### Item 4 decided, 2026-08-23 — package classes and what automation may touch

Read from `/var/log/apt/history.log` on 2026-08-23 (read-only), not inferred.
**98 of the 107 transactions in the host's whole history are
`/usr/bin/unattended-upgrade`.** Automation is not an edge case here; it is how
this host is almost always patched.

`Allowed-Origins` in `/etc/apt/apt.conf.d/50unattended-upgrades` is base +
`-security` + the two ESM pockets. **`-updates` is not included**, which is why
15 packages were still listed upgradable *after* the August window's automatic
run finished: `cloud-init`, `netplan.io`, `fwupd`, `qemu-user-static` and
friends are `-updates`-only and no automation will ever apply them.

| Class | Who applies it today | Forces | Why it is its own class |
|---|---|---|---|
| **Security** (`-security`, ESM) | unattended-upgrades, automatically | Usually nothing | The routine case. The August window's own run was 8 transactions of exactly this: `tzdata`, `ncurses-base`, `libpam-runtime`, `systemd`, `dnsmasq-base`, `perl`, `python3.10`, `distro-info-data` |
| **Ordinary** (`-updates`) | **Nobody, until an operator does** | A reviewed manual `apt-get upgrade` | These accumulate silently and indefinitely. This is the class a monthly window exists to drain, and the reason "no updates required" is rarely the true answer |
| **Container runtime** (`docker.io`, `containerd`, `runc`) | unattended-upgrades *could* — see below | Compatibility review, then a deliberate apply | A daemon upgrade restarts containers. Held from 2026-08-23 |
| **Kernel** (`linux-*-oracle`) | unattended-upgrades, automatically | A reboot, operator-timed | Installs **inert**: the new kernel sits in `/boot` and changes nothing until a reboot the operator chooses. Two (`1050`, `1054`) had queued this way before the window and harmed nothing |
| **Restart-required** | — | Recreating named services | Needs the running-set manifest to know which |

**Decision: hold `docker.io` only. Kernel and security updates stay
automatic.** Kernel installs are inert until an operator-controlled reboot, so
automation there buys security with no runtime risk, and holding it would also
destroy `reboot-required` as a signal.

**The justification is prospective, and this is worth stating plainly because
the original one was wrong.** The case for holding Docker was "automation
already broke you once." It did not: as the corrected bullet in
[the evidence section](#the-production-evidence) records, `docker.io` has never
been upgraded on this host. The case that survives is forward-looking and still
real — `docker.io` 29.1.3 is published in `jammy-security/universe` as well as
`jammy-updates/universe`, and `jammy-security` **is** an allowed origin, so the
next Docker security update would be applied automatically, restarting the
daemon and every container under it, at whatever hour the timer fires. That is
worth preventing. It is a smaller claim than the one it replaces, and if the
maintainer would rather not carry the hold on that basis alone, reversing it is
`sudo apt-mark unhold docker.io` and a line in this table.

The cost is explicit: a held package receives no automatic security fixes.
`apt-mark showhold` therefore becomes a **preflight line item** — an unreviewed
hold is how a package silently rots — and draining the held class is part of
what a window is for.

**Ordering fix, from item 1.** Do not restore the apt timers until after the
resume gate. The August window restored them in the same command that refreshed
and simulated, and `unattended-upgrades` started three minutes later, inside the
window. Stage 2's script owns this ordering; it is not a matter of remembering.

#### Item 5 decided, 2026-08-23 — the host checkpoint directory

**`/var/lib/cartracker/maintenance/`**, root-owned, `0755` on the directory and
`0644` on files.

On the **root filesystem, deliberately** — not `/mnt/data`. "`/mnt/data` did not
come back" is one of the failures this file exists to help diagnose, and a
checkpoint stored behind the mount you are diagnosing is worthless. `/` was at
65% of 49 GB before the August window and the file is bytes.

`0644` so it is readable from a console session without `sudo`, which is
friction in exactly the moment there is none to spare.

**Append-only, one line per phase transition**, so an interrupted window leaves
a trail rather than a single overwritten "current" value. Each line carries
phase, UTC timestamp, git revision, running kernel, and the running-set
manifest's path.

**Never secrets.** Stronger than the usual version of that rule here, because
this file is designed to be read under pressure and screenshotted into a chat.
Stage 2's tests assert the writer emits only the five fields above.

Postgres remains authoritative for maintenance state whenever it is up. This
file is a breadcrumb for the window in which it is deliberately down, and the
two must never be reconciled in the direction of the file.

No production maintenance mode is declared in this stage.

#### Item 1 done, 2026-08-23 — the window was recovered verbatim

[`docs/runbooks/runbook_host_maintenance.md`](../runbooks/runbook_host_maintenance.md).
It was going to be a reconstruction with the command sequence marked as gaps;
it is instead the real thing. The window was driven from **Codex**, not Claude
Code, which is why the Claude transcripts have a three-day hole across it
(2026-08-15 06:34 → 2026-08-18 11:46) and no `apt`, `dpkg` or `systemctl`
anywhere in them. The session survives at
`~/.codex/sessions/2026/08/17/`, thread *"Complete plan 135 stage 5"*, opening
with "I've just merged stages 4 & 6 of plan 135. I know it relies on some VM
runs."

Recovered: 189 shell invocations with timestamps and output — full preflight,
the stuck-`apt-daily` diagnosis, the controlled transaction, the reboot, host
validation, restore, and stack verification. Downtime was **~3 minutes**
(reboot 04:21:54, boot 04:22:27, `up -d` 04:23:19, verified 04:24:53), inside a
~27-minute window.

**Three things the record adds that the evidence section above did not have:**

1. **The apt index was 68 days stale** (last refreshed 2026-06-11), so the
   preflight `apt list --upgradable` was computed against a June catalogue and
   was not the transaction that ran. The runbook makes reading `APT_LIST_AGE`
   before trusting `UPGRADABLE` an explicit step.
2. **Restoring the apt timers is what started `unattended-upgrades` mid-window**
   — the evidence section says this happened, and the transcript shows *why*:
   stop-refresh-simulate-restore was issued as one command, so the timers came
   back three minutes before the reboot. The fix is ordering: restore them after
   the resume gate, not before. `apt-mark showhold` was empty, so nothing
   protected Docker or the kernel from that run.
3. **The profile-gated restore failure is observed, not theoretical.** After
   `docker compose up -d --force-recreate`, `trawl` and `redis-trawl` were still
   down; they came back only from a second, explicitly-named command. This
   upgrades their `profile-running` entries in
   [`maintenance-running-set.txt`](../../maintenance-running-set.txt) from
   reconstructed to **observed in this window**, and it is the same failure the
   registry was written to prevent.

**One correction owed to this plan's evidence section**, which says the Docker
upgrade broke Promtail and implies it happened here. The transcript does not
establish when `docker.io` reached 29.1.3: it was absent from the stale
preflight list, and by 04:25 Installed already equalled Candidate. Either it
upgraded in the window and the review missed it, or it moved silently on an
ordinary day and was found by luck. Both argue for holding Docker; the second
is worse, and settling it needs `/var/log/apt/history.log`. Recorded in the
runbook's §8 rather than resolved.

Kernel `6.8.0-1049-oracle` → `6.8.0-1058-oracle`, with `reboot-required`
already pending for `1050` and `1054` before the window opened.

**Item 1 is complete against its own spec** — timeline, commands, failure
modes, intended-stopped services and recovery evidence are all present from the
primary record. One thing first written up as a gap was settled on re-reading
rather than left open: the pre-window disk baseline exists (`/` at 65%,
04:06:23).

Two items genuinely remain on the host, neither required by item 1. The package
transaction detail (`/var/log/apt/history.log`) is enrichment. **When
`docker.io` reached 29.1.3 is not** — it decides whether Docker gets an
`apt-mark hold`, so it is an input to item 4 and should be read before that
decision is made.

One new finding, unrelated to maintenance and unowned: `netplan generate`
warns that `/run/netplan/enp0s6.yaml` is "too open" and "should NOT be
accessible by others". It validates, so it blocked nothing, and it appears in no
plan.

#### Item 2 done, 2026-08-23 — and it found two things

The inventory landed as [`maintenance-running-set.txt`](../../maintenance-running-set.txt),
in `healthcheck-exemptions.txt`'s format and read by that file's parser, with
`tests/test_maintenance_running_set.py` checking it against the Compose
sources. **It records exceptions only**: the 28-service restore set is derived
from `docker-compose.yml`, so a new service is expected running by default and
has to be named to be anything else. A second hand-maintained copy of the
service list is how the first one goes stale, which is the argument Plan 144
already made for the two sibling registries.

Six classes, and the two that are *not* exclusions carry the plan's whole
point: `profile-running` (restored, but only if the profile flag is passed) and
`restart-gap` (expected running, does not restore itself). `oneshot` is a third
distinction that matters — `flyway` and `airflow-init` **do** run under `up -d`
as `service_completed_successfully` dependencies; a resume gate that waits for
them to be *running* waits forever.

**Finding 1 — `caddy` does not come back after a reboot.** It serves `:80` and
`:443` for cartracker.info and declares no `restart:` key
([docker-compose.yml](../../docker-compose.yml), `caddy`), so its effective
policy is `no`. It is the only long-running default-project service with this
gap. Planned maintenance is unaffected, because Stage 3 restores from the
manifest rather than from restart policies — but an *unplanned* reboot leaves
the public site down until someone runs `up -d` by hand.

**And nothing reports it**, which is the half that makes this worth pulling
forward. A stopped container does not read as unhealthy; it leaves the metric
altogether. `DockerApi.inspect_project_containers`
([container_health/docker_api.py](../../container_health/docker_api.py)) filters
`status` to `running`, `restarting`, `paused` — deliberately, so one-shots do
not publish a meaningless health state — and `health_values` raises only when
the fleet is *entirely* empty. One absent service is therefore silent by
construction. This plan already named the failure without having an instance of
it: the Stage 3 stack gate demands "neither unhealthy nor unconfigured services
hidden as absence." This is what that sentence looks like in production.

The one-line fix was **moved to Stage 0 item 7 on 2026-08-23** rather than left
to Stage 2. Until it lands the test asserts the registry keeps saying so.

**Finding 2 — Plan 140 is structurally blind to the auxiliary projects, so
this plan cannot delegate that check.** Success criterion 5 and the Stage 3
stack gate both require "intentionally stopped auxiliary services still
stopped". Plan 140's metric cannot answer it: `health_values` in
[container_health/collector.py](../../container_health/collector.py) filters on
`com.docker.compose.project` and drops everything that is not `cartracker`.
That filter is correct and must not be relaxed — its docstring records why,
from the Plan 140 Stage 1 soak, which found four stale `unhealthy` containers
(`cartracker-lakekeeper`, `-lakekeeper-migrate`, `-lakekeeper-postgres`,
`cartracker-mlflow`) that a "has a compose label" filter would not have
excluded. The consequence for this plan is precise: **the auxiliary-still-
stopped gate is Plan 142's own check against this manifest**, not a Plan 140
health reading, and Stage 3 must implement it separately. The same docstring
supplies the fixture the plan asked for — "`up -d` on either sibling project
brings the condition straight back" is the observed proof that restoring
everything Compose can find is wrong.

**Still open on item 2.** `COMPOSE_PROFILES` appears nowhere in the repository,
so which profiles production actually has enabled is unrecorded in source. The
two `profile-running` entries are reconstructed from checked-in evidence —
`_PROFILE_GATED_IN_SCOPE = {"trawl", "redis-trawl"}` in
`tests/test_observability_config.py`, whose comment cites the 2026-08-14 solver
outage, plus Plan 136 Stage 3a deploying against a running production `trawl`
on 2026-08-23 — and are marked in the file as reconstructed rather than
observed. Confirming them against the live host is a read-only check owed
before Stage 2 builds the restore step on top of them.

### Stage 1 — Replace deploy intent with scoped coordination and truthful drain

1. Add the single coordination record/API with kind, immutable expanded scope,
   phase, evidence fields, and legal transitions. Only host maintenance may
   enter an offline interval.
2. Keep `/deploy/*` as a compatibility facade and dual-signal legacy consumers
   until every DAG, long job, admin page, and deploy script reads the new
   contract. Remove the old table/sensor only in a later contract slice.
3. Add a checked-in service-to-surface registry, dependency/follower expansion,
   explicit execution lifecycle, and per-DAG admission declarations. Reject
   unknown targets, surfaces, or lifecycle classes. Lifecycle describes current
   production behavior; it does not anticipate Plan 152's migration.
4. Replace the 600-second global deploy sensor with an indefinite rescheduling
   gate that blocks only intersecting scopes. A stale-state alert, not a DAG
   timeout, notifies the operator.
5. Replace the ambiguous aggregate with named, scoped active-work counts and
   oldest-start timestamps. Encode admission and drain sets separately; pending
   durable backlog never blocks, and unknown evidence fails closed.
6. Permit `draining -> active` only after the gate is observed effective and
   every scoped drain count is zero on a confirming read. Add explicit cancel,
   begin-validation, and complete operations with legal-transition tests.
7. Add stale-coordination and gate-health metrics/alerts. Stale means “needs a
   human,” never “safe to resume.”

Verify first with a targeted deploy whose scope does not include `trawl`, then
with a non-outage whole-production dry run: prove unaffected surfaces continue,
affected admission stops, admitted work drains, durable backlog remains allowed,
and release produces neither failed nor duplicated mutations.

**Review decision, 2026-08-25:** the targeted path is now executable through
the scoped `/deploy/*` compatibility facade and `redeploy.sh`; the runbook owns
the walkthrough and abort. The whole-production dry run is explicitly gated on
Stage 3's validation guard. No temporary force-complete exists: reaching
`validating` without release evidence must remain fail-closed.

#### Stage 1 first production run — 2026-08-25 — it hung, then it worked

**Deployed 19:52 UTC.** The contract had never executed against a real database
before that evening; the unit suite's 950 passing tests all ran against mocks.
Its first real invocation **hung**, and two independent causes had to be removed
before it could authorize anything.

##### Defect 1 — three drain queries named the wrong schema

`redeploy.sh` looped `409` every 5s and never exited. It was not waiting for
work; it could not *observe* work. Ten of twelve sources read `unknown`, and
unknown fails closed by design, so the loop was unreachable by construction.

| Source | Query named | Table actually in |
|---|---|---|
| `running_detail_claims` | `public.detail_scrape_claims` | **`ops`** |
| `airflow_task_instances` | `task_instance` unqualified | **`airflow`** |
| `airflow_gate_observations` | `dag_run` unqualified | **`airflow`** |

The ops role's `search_path` is `ops, staging, public` — `airflow` is not on it.
`_database_count` catches the error and returns `unknown`, so the operator saw
"In-scope work is still draining", never "your SQL is wrong". **A fail-closed
gate that cannot distinguish busy from broken will present a defect as
patience.** Fixed by schema-qualifying all four tables and adding SQL smoke
tests that execute the real query strings; the structural reason no test caught
it is [Plan 139](plan_139_test_suite_maintenance.md) **Stage F**.

##### Defect 2 — the contract could not gate the deploy that installed it

The other seven `unknown` sources were not a bug. `_service_jobs` reads
`active_by_surface` from each service's `/ready`, and `_container_processes`
reads `/oneoff-processes` — **endpoints that only exist after the deploy the
drain is gating.** Verified live on the pre-deploy fleet: `archiver` and
`processing` returned `{"ready": true}` with no `active_jobs`; `scraper`
returned `active_jobs` but not the per-surface breakdown; `container-health`
returned **404**.

So fixing Defect 1 was necessary but not sufficient: **Stage 1 could not
authorize the deploy that installs Stage 1.** The bootstrap used
`/deploy/start` → mutate → `/deploy/complete`, skipping only `begin-drain` and
`authorize`. That keeps the admission gate up throughout — the sensor blocks on
`phase IN (requested, draining, active, validating)` — and forgoes only the
drain wait, which was an acceptable trade with a verified-quiet fleet.

> **This is a migration artifact, not a property of the contract.** A fresh
> environment does not hit it: an empty host comes up from current code, every
> service exposes its job counters from the first boot, and all twelve sources
> read `known` immediately. [Plan 121](plan_121_staging_environment.md)'s
> staging host is therefore **not** affected, and neither is any rebuild from
> empty. What made tonight different is that the drain contract and the
> endpoints it interrogates shipped in the *same release*, and that release ran
> against a fleet still on the previous build.
>
> **The recurring case is narrower and worth guarding.** It returns whenever a
> future release adds a drain source whose evidence endpoint ships in that same
> release — the new `ops` interrogates services that do not expose it yet, every
> such source reads `unknown`, and the deploy hangs exactly as it did here. So
> the rule for anyone extending `DRAIN_SOURCES`: **a new source that depends on
> a new service endpoint cannot be enabled in the release that introduces the
> endpoint.** Ship the endpoint first, deploy it, then enable the source — or
> accept a one-time bypass and say so in the PR.
>
> The bypass used tonight is the fallback when that ordering was not followed:
> `/deploy/start` → mutate → `/deploy/complete`, which keeps the admission gate
> up and forgoes only the drain wait. It is safe **only** when the operator has
> confirmed the fleet is quiet by other means, as was done here.

##### Defect 3 — `ct-coordination-stale` fired on the healthy steady state

Shipped in the same PR and **firing within minutes of deployment**, reporting
`Alerting (NoData)` while `coordination_state` read `phase=none, generation=6`.
Its selector is `phase!="none"`, which matches nothing when no window is open —
and `noDataState: Alerting` with `for: 0s` turned that absence into a page. It
fired permanently *except* during the seconds a deploy was in flight.

Grafana retains the last real series' labels for a NoData instance, so the
notification read *"deploy coordination has remained requested for over 30
minutes"* about a window released twelve minutes earlier. **Fixed to
`noDataState: OK`** (PR #249). An audit of all 22 alert rules found the
inversion isolated to this one.

##### Then it worked, end to end

After the fix, all twelve sources reported `known` with zero blockers — the
first time the contract was fully observable — and the full lifecycle ran:

```
begin-drain   -> phase=draining, drained=true, blockers=[]
authorize     -> HTTP 200, phase=active          <- the call that looped forever
begin-validation -> phase=validating
deploy/complete  -> phase=none, generation=4, deploy_intent=none
```

Two further confirmations followed unprompted: `redeploy.sh --restart` drove
the same lifecycle cleanly for the Prometheus/Promtail/Grafana config restart
and again for the Grafana alert deploy, both printing "Drain confirmed; deploy
mutation authorized".

**The drain also proved truthful under live load rather than merely at rest.**
One read landed inside the `scheduled__2026-08-25T19:45:00` detail run and
returned `running_detail_claims: known, count=400`; the next, after the run
finished at 19:46:12, returned `0`. Claims are transient, so the 1,869
`processed` rows still in `ops.detail_scrape_claims` are April residue rather
than live state — worth knowing before anyone reads that table as current.

> **Recorded against the soak-clock error made the same evening:** CAR-7 was
> marked "Soaking" when PR #243 merged at 14:24, but production ran PR #241
> until 19:52. **A merge is not a deploy**, and roughly five hours of that
> window measured code running nowhere. Plan 141 hit the identical error.

### Stage 2 — Build the checked-in host procedure

**First slice built 2026-08-25:**
[`scripts/host_maintenance.py`](../../scripts/host_maintenance.py) provides the
safe online lifecycle through `begin-validation` and the append-only host
checkpoint. Transition commands are replay-safe across the split outcome where
Postgres advances but the local checkpoint write fails. It exposes no stop,
package, reboot, restore, or complete command yet; those require the remaining
Stage 2 mechanics and Stage 3's release evidence respectively.

**Preflight slice built 2026-08-25:** the same client now exposes a read-only
`preflight` command. It fails closed on apt/dpkg locks, package-database
inconsistency, or package-hold drift,
and writes a non-secret evidence bundle containing the host baseline, sanitized
Compose renders, and the actual running-set manifest. The manifest records
Compose identity, profiles, image IDs/digests, runtime/health state, restart
policy, and Docker log configuration without persisting interpolated environment
variables. Stop, update, reboot, restore, and complete remain absent.

**Running-set round-trip slice built 2026-08-25:** the client derives an exact
per-project stop/start plan from that manifest. Only services captured as
running are selected; profile-gated services retain their required Compose
profiles, while one-shot, on-demand, deliberately paused, and foreign services
remain excluded. Contradictory manifests fail closed. Unit coverage round-trips
default, profile-running, one-shot, on-demand, and paused auxiliary examples.

**Manifest-scoped stop/start slice built 2026-08-25:** `stop` and `start`
consume only that derived plan, carry Compose profiles and source files, verify
every selected container's resulting Docker state, and append `stopped` or
`started` only after the postcondition holds. Their authority is the latest
API-confirmed offline checkpoint, so both commands remain replayable while the
coordination API and Postgres are deliberately offline.

**Package-preparation slice built 2026-08-25:** `prepare-update` refreshes apt
indexes, requires the operator to name every held package explicitly, resolves
ordinary plus held upgrades, re-simulates the combined version-pinned
transaction, and downloads it without installation. The resulting
`package-plan.json` is content-addressed and calls out container-runtime,
kernel, SSH, and network compatibility boundaries for review before `update`.

**Offline package-apply slice built 2026-08-25:** `update` requires the
`stopped` checkpoint, matching manifest, exact package-plan digest, and explicit
apply, release-note, and compatibility-review confirmations. It records the apt
automation enablement state, masks the timers and unattended-upgrade service,
applies only the pinned argv, verifies installed versions and `dpkg --audit`,
rechecks apt/dpkg locks immediately before masking, proves the reviewed hold set
survived, syncs filesystems, writes `update-result.json`, and only then checkpoints
`updated`. Apt automation intentionally remains masked until the Stage 3 resume
gate has passed.

**Reboot-boundary slice built 2026-08-25:** preflight records the Linux boot
ID. `reboot` requires the `updated` checkpoint and explicit confirmation,
syncs, checkpoints `rebooting`, and then invokes systemd. A post-boot replay
must observe a different boot ID before it records `rebooted`; command return
alone is never treated as reboot evidence.

**Stage 2 ordering slice built 2026-08-25:** `plan` emits the canonical local
procedure through `begin-validation` without executing commands, `drain` matches
the plan's public command name while retaining `begin-drain` compatibility, and
preflight now checkpoints only after its evidence bundle exists. Tests pin the
phase order and prove Stage 2 has no implicit or exposed `complete` path.

**Durable-history slice built 2026-08-25:** V044 adds the append-only
`staging.coordination_state_events` record, every native and compatibility-
facade state mutation writes exactly one event in the same transaction, and the
existing staging-event archiver registry flushes it to Parquet. Normal
transitions and refusals are narrated through Plan 141's structured-field
contract. Bounded drain-progress narration and the completion checkpoint remain
open below.

Add an operator-run script, proposed as `scripts/host_maintenance.sh`, with
idempotent subcommands rather than one irreversible monolith:

```text
preflight -> request -> drain -> wait-active -> stop -> update -> reboot
          -> start -> begin-validation -> validate-host -> validate-stack
          -> complete
```

The script prints and checkpoints every phase. It never stores credentials and
never calls `complete` from a general exit trap.

#### Preflight and package preparation

- verify SSH and validate the SSH and network configurations before risking
  network changes;
- record Git revision, kernel, `/etc/os-release`, `reboot-required`, disk bytes
  and inodes, mount UUIDs, Docker version/config, failed systemd units, and
  package holds;
- inspect apt/dpkg locks and processes; never kill or interrupt an active
  `dpkg` transaction;
- render every Compose configuration and capture the running-set manifest;
- refresh and download packages while live where safe, then review the exact
  transaction and release notes;
- treat Docker/containerd, kernel, SSH, and networking packages as explicit
  compatibility boundaries;
- pull required images before pausing production and validate collector/daemon
  API compatibility when Docker changes.

During controlled apt work, stop/mask the apt daily and unattended-upgrade
timers/services in a recorded, reversible way. Restore and verify them after the
operator-controlled transaction; do not let restoring them start a competing
upgrade inside the window.

#### Stop and reboot

Use `docker compose stop` for ordinary maintenance so containers, networks, and
their configuration are preserved. Use `down` only when the reviewed change
requires recreation. Stop every running Compose project from the manifest,
including profiles; do not start projects that were intentionally stopped.

Before reboot, sync filesystems and record the installed kernel/package result.
The reboot itself always requires explicit operator confirmation.

#### Durable transition history

Added 2026-08-25 from the Plan 141 logging health check. This stage already
committed to append-only history and put it in two places, neither complete:

| | `coordination_state` (Postgres) | `history.jsonl` (host) |
|---|---|---|
| Shape | single row, mutated in place | append-only |
| Survives a host rebuild | yes | no — `/var/lib/cartracker/maintenance` is on root |
| Queryable and joinable | yes | no |
| Records completion | phase timestamps only | no — `none` is not in `CHECKPOINT_PHASES` |

[`V043`](../../db/migrations/V043__coordination_state.sql) pins
`coordination_state` to one row (`CHECK (id = 1)`). When `generation`
increments, the prior window is gone. The same migration applies the opposite
and correct reasoning one table over, for `coordination_gate_observations`:
*"Historical rows are harmless and make the proof auditable."*

The split is already known to this plan — the first Stage 2 slice is described
as replay-safe across "the split outcome where Postgres advances but the local
checkpoint write fails." That designs around the divergence rather than
removing it. Two later commitments depend on removing it: **Stage 4** must
capture planned-versus-actual phase duration and drain time-to-zero, which
cannot be compared across windows from one overwritten row; and **CI invariant
2** asserts legal predecessor transitions, which an event log makes checkable
against production rather than only against unit tests.

[Plan 151](plan_151_distributed_tracing_and_runtime_topology_audit.md) states
the governing principle independently: telemetry may be lossy, control state may
not be. Coordination state satisfies that for its present value and fails it for
its history.

The work:

1. [x] Add `staging.coordination_state_events` — append-only, `bigserial`
   primary key, one row per transition carrying `generation`, `prior_phase`,
   `phase`, `kind`, actor, and timestamp. Grants follow V043. **Built in V044;
   actor is the coordination request's durable `requested_by` identity.**
2. [x] Write the event in the **same transaction** as the
   `coordination_state` update, so a mutation cannot succeed without its history
   row. **Built for native coordination mutations and the `/deploy/*`
   compatibility facade; tests prove a failed event insert rolls back the state
   mutation.**
3. [x] Register it in
   [`archiver/processors/flush_staging_events.py`](../../archiver/processors/flush_staging_events.py) —
   one entry naming table, pk, columns, and `minio_prefix`, matching the five
   already there. Flush is snapshot → Parquet → `DELETE WHERE pk <= max_pk`.
4. [x] Resolve the two records: `history.jsonl` remains an offline operator
   convenience, written only after the Postgres-backed API confirms a phase. It
   contains host-only evidence, is never reconciled into Postgres, and is not a
   second transition-history authority. Dropping it would remove the only
   breadcrumb readable while Postgres and `/mnt/data` are deliberately offline.
5. [ ] Extend checkpoint coverage to the completion transition. **The Postgres
   event schema supports it now; the local checkpoint remains blocked with the
   `complete` command on Stage 3's validation-evidence guard.**
6. Narrate transitions.
   - [x] Log every normal native and compatibility-facade transition once with
     `kind`, `phase`, `prior_phase`, and `generation`.
   - [x] Log refused legal-transition and drain-authorization attempts at
     `WARNING`.
   - [x] Log drain progress at a bounded interval and log client timeouts, not
     per poll. **The `wait-active` client polls every five seconds without a
     short overall deadline, emits progress at most once per minute by default,
     and records request method/route when the API itself times out.**

Field-carrying log records depended on Plan 141's formatter change, which now
lets `extra=` survive into emitted JSON. The completed narration items use that
contract rather than embedding fields into free-form messages.

This is expand-only. Every existing reader of `coordination_state` is
unaffected, and the stage reverts by reverting the migration and the registry
entry.

### Stage 3 — Make Plan 140 the resume gate

Host validation occurs before application validation:

1. expected kernel booted and no unexpected `reboot-required` remains;
2. root and `/mnt/data` are mounted from the expected devices with safe
   byte/inode headroom;
3. network, DNS, clock synchronization, SSH, Docker, and required systemd units
   are healthy;
4. Docker daemon log limits and storage paths remain effective;
5. no apt/dpkg transaction or failed package configuration remains.

Then restore only the manifest's intended running set. The stack gate requires:

- every expected service present;
- every in-scope Plan 140 health state healthy, with neither unhealthy nor
  unconfigured services hidden as absence;
- Postgres, MinIO, Airflow, ops, Prometheus, Grafana, Loki, and Promtail direct
  readiness checks;
- fresh Prometheus scrapes and log ingestion, without Promtail replay/error
  storms (reuse Plan 141's contract once available);
- deploy/maintenance APIs reporting the expected state;
- intentionally stopped auxiliary services still stopped.

Only after every required gate passes may an operator explicitly complete
maintenance. A failed gate leaves the system in `validating`, with work paused.

### Stage 4 — First scheduled execution and handoff

Run one reviewed security/package maintenance window using the checked-in
procedure. A second person is not required, but the plan, package transaction,
rollback points, and expected outage are reviewed before execution.

Capture:

- planned versus actual phase duration;
- drain counts and time to zero;
- packages and kernel before/after;
- downtime and service-health convergence time;
- any manual command not represented by the script;
- alerts/notifications sent;
- explicit resume evidence and post-resume DAG behavior.

Every undocumented manual step either becomes a script/runbook step or receives
a written reason for remaining judgment-based.

## Rollback and recovery

Rollback is phase-specific:

- **Before stop:** cancel maintenance and resume normally.
- **Package install before reboot:** finish `dpkg` to a consistent state; use
  reviewed cached versions/holds for rollback rather than interrupting it.
- **Boot failure:** follow the provider recovery procedure and select the
  previous kernel where available.
- **Docker incompatibility:** restore the reviewed Docker/containerd package
  versions and daemon configuration, then validate Compose before starting.
- **Application regression:** keep maintenance active, return to the prior Git
  revision/images, and recreate only the affected services.
- **Mount/storage mismatch:** do not start stateful services; diagnose from the
  host/console with the manifest and `fstab` evidence.

No rollback path includes `git reset --hard`, deleting Docker volumes, `docker
volume prune`, or automatically releasing maintenance intent.

## Tests and CI invariants

1. Only one coordination operation exists; kind and immutable expanded scope
   determine its legal phases and release gates.
2. Every state transition accepts only its legal predecessor and is idempotent
   where operator retry is expected.
3. Pending backlog does not block drain; processing/running claims do.
4. A long coordination window does not fail DAGs merely because time passes,
   and unaffected surfaces continue running.
5. The running-set manifest round-trips default, profile-gated, auxiliary, and
   intentionally stopped services.
6. Script dry-run tests assert phase ordering and prove no failure path calls
   `complete` implicitly.
7. Plan 140 coverage is a declared dependency of the resume gate; missing
   health data fails closed.
8. Package/reboot commands require explicit non-dry-run confirmation.
9. Every `coordination_state` mutation writes exactly one history row in the
   same transaction; a failed write leaves neither.
10. Every phase transition reachable in the coordination modules has a
    corresponding event type, so a transition cannot be added silently.
11. `staging.coordination_state_events` flushes through the existing archiver
    registry, not a bespoke path.
12. A completed window is reconstructable from Postgres after its `generation`
    has been superseded.

## Intersections and sequencing

### Plan 140 — mandatory dependency

Plan 140 defines healthchecks, the three-state container-health metric, and CI
coverage. Plan 142 consumes that signal as its stack-resume gate. It neither
adds healthchecks nor creates a competing service allowlist. Stage 0-2 design
can proceed earlier, but Stage 3 cannot be accepted before Plan 140's metric has
soaked and its coverage is trustworthy.

### Plan 136 — drain and authority patterns

Plan 136 owns solver efficacy, threshold-gated `trawl` recycling, and narrowly
allowlisted Docker restart authority through a socket proxy. Reuse its
claim-count semantics: its recycle keeps `active_jobs == 0` as a precondition,
and the named counts below remain the authority on whether work is actually in
flight. Do not extend its application endpoint into host package or reboot
authority: Plan 142 remains an SSH/console operator procedure.

**Two things changed on 2026-08-23 and this plan inherits both.**

Plan 136's Stage 3 no longer specifies a drain protocol. The pause-claiming,
poll-for-idle, restart, resume sequence this plan expected to reuse was
replaced by an **Airflow pool**, which is global across DAGs: every
solver-consuming task takes one slot and the recycle takes them all, so the
scheduler enforces mutual exclusion instead of an application protocol doing
it. That is a better fit for this plan's gate than what it replaced — see
[Stage 0](#stage-0--turn-the-successful-window-into-fixtures-and-decisions),
where it is now the third option to prototype.

And the restart authority is **two** proxy instances, not one grant with an
added verb. `ALLOW_RESTARTS` narrows nothing once `CONTAINERS=1` is set, so the
read grant and the restart grant cannot share an instance; the restart proxy
runs `CONTAINERS=0, POST=1, ALLOW_RESTARTS=1`. This strengthens the non-goal
above rather than complicating it: that proxy can issue `stop`, `restart` and
`kill` and **structurally nothing else**, so it cannot be widened into reboot
authority by editing an allowlist. If this plan ever needs host-level
authority, it needs a different mechanism, which is the intended answer.

**Sequencing reversed 2026-08-23.** Plan 136 was ahead on the strength of its
production failure signals; those shipped (Stages 0 and 2, and 3a), and Plan
136 is now blocked on a memory-baseline soak until 2026-08-25 while this plan
is workable. So Plan 142 goes first and **establishes** the pool gate; Plan
136's Stage 3d inherits the *mechanism*, not the pool itself — Phase A of item 3
found that a task belongs to exactly one pool, so `solver` has to be its own,
sized 2 for mutual exclusion against `maintenance`'s 16. What 136 inherits is
the pattern and the verified scheduler behaviour; what 142 owes in return is
that its hold sets both pools to 0.

### Plans 135 and 141 — storage and logging checks

Plan 135 supplies disk/inode checks, bounded Docker/journald behavior, and the
storage-maintenance evidence format. Its current runbook remains the monthly
storage procedure, not a whole-host playbook. Plan 141 supplies the no-replay,
label, and ingestion-volume checks after Promtail or Docker replacement.

### Plan 125 — intentionally stopped auxiliary services

The paused MLflow/Lakekeeper projects are the required proof that maintenance
restores intended state, not “everything Compose can find.” Their state changes
only when Plan 125 authorizes it.

### Plans 108 and 121

Plan 108's remote deploy-trigger concept does not gain host-maintenance
authority. Plan 121 staging may later exercise most of this workflow, but a
single-host reboot and mount validation still require a production canary
window because staging cannot prove the production VM's firmware, kernel, disk,
or network path.

## Success criteria

1. One coordination state replaces deploy intent, survives stack stop/reboot
   for host maintenance, and never releases itself after an ambiguous failure.
2. New in-scope DAG work pauses without timeout failures while unaffected work
   continues; admitted work drains to named zero counts that exclude pending
   backlog.
3. The pre-maintenance running/stopped set is captured across every Compose
   project/profile and restored exactly.
4. Package preparation, controlled apt execution, reboot, host validation, and
   rollback are executable from a checked-in, idempotent operator procedure.
5. Plan 140 health plus direct stateful/observability checks gate explicit
   resume; any missing or failed signal keeps production paused.
6. The first scheduled window completes with no data loss, no unintended
   service activation, no maintenance-caused DAG failures, and a complete
   before/after evidence record.

## Operational cadence after completion

Run a reviewed host-maintenance check monthly and execute a window when security
updates, a required kernel/runtime fix, or accumulated ordinary updates justify
it. “No updates required” is a valid monthly result. Emergency security work may
compress preparation time, but it does not bypass maintenance intent, drain,
explicit reboot authority, or the resume gate.
