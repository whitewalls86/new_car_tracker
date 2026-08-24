# Plan 142: Planned Host Maintenance and Production Quiescence

## Status

DRAFT, written 2026-08-18 after the first deliberate whole-host maintenance
window exposed that the repository has a deploy procedure and a storage
runbook, but no durable procedure for pausing production, updating Ubuntu,
rebooting the VM, proving the host and stack healthy, and safely resuming work.

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
- The Docker package upgrade raised the daemon's minimum API version and broke
  Promtail 2.9.8 discovery. The host update therefore changed an application
  compatibility boundary even though no application code caused it.
- Recovery required checking the running kernel, `/` and `/mnt/data` mounts,
  Docker configuration, selected services, Loki/Promtail ingestion, and the
  intentionally stopped Plan 125 services before work could safely resume.

The outage was successful, but the procedure lived in the conversation. The
next maintenance window should be an execution of checked-in policy rather than
a reconstruction of it.

## Why deploy intent is not maintenance intent

Deploy intent protects a short service replacement:

- expected duration under ten minutes;
- most of the host stays available;
- the ops API can release intent;
- an exit trap releasing intent is usually safer than leaving it stuck;
- a targeted `docker compose up -d SERVICE` is the normal mutation.

Host maintenance has the opposite properties:

- package work and reboot duration are variable;
- Postgres, Airflow, ops, and the intent API are deliberately offline;
- the kernel, mounts, network, Docker daemon, and every Compose project may
  change state;
- automatic release on timeout or script exit could resume work onto a broken
  host;
- rollback may require the Oracle Cloud console or an older kernel/package.

Maintenance therefore needs a separate durable state and a separate operator
workflow. It may reuse deploy-intent primitives, but it must not be implemented
as “deploy intent with a larger timeout.”

## Goals

1. Stop new production work without turning an expected pause into failed DAGs.
2. Drain active work using counts that distinguish queued backlog from work that
   is actually mutating state.
3. Preserve the intended running/stopped state across a whole-stack stop and
   host reboot, including profile-gated and auxiliary Compose projects.
4. Make Ubuntu package updates and reboot a normal, reviewable operation.
5. Refuse to resume until the host and Plan 140 service-health contract pass.
6. Keep maintenance active after any ambiguous failure; release is always an
   explicit operator action.
7. Capture enough before/after evidence to diagnose or roll back the change.

## Non-goals

- Zero-downtime host patching. This is a single production VM and the window is
  intentionally user-visible.
- Automatic package installation or unattended reboot.
- Giving an application container unrestricted host or Docker authority.
- Replacing targeted deploy intent or `scripts/redeploy.sh`.
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

Package refresh, compatibility review, image pulls, disk checks, Compose
rendering, and console-access verification happen before production pauses.
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

## Maintenance state machine

The exact schema is decided in Stage 1, but the externally visible states are:

| State | Meaning | Allowed transition |
|---|---|---|
| `none` | Normal scheduling and claims | `requested` |
| `requested` | New work is gated; existing work may drain | `drained`, `none` (cancel) |
| `drained` | Authoritative active-work counts are zero | `offline`, `none` |
| `offline` | Stack stop/reboot is authorized and expected | `validating` |
| `validating` | Host and services are back, work remains paused | `none` only after all release gates pass |

Deploy and maintenance intent are mutually exclusive. Starting either while the
other is active returns a conflict with the existing state; no stale-lock
takeover silently converts one kind into the other.

The maintenance record includes requester, reason, requested/start timestamps,
phase timestamps, expected work, running-set manifest location, and operator
notes. A stale-state alert pages an operator but does not release the state.

## The drain contract

`number_running` is replaced for maintenance purposes by named counts:

- artifacts with `status='processing'`;
- detail claims with `status='running'`;
- active Airflow task instances that can mutate production;
- long-running pack/prune/maintenance endpoints still inside a non-interruptible
  section;
- any service-specific claim introduced later.

Pending artifacts are durable backlog and **do not block a stop**. Each count
has its own oldest-start timestamp so one stuck claim is identifiable.

During `requested`, new DAG work reaches a maintenance-aware rescheduling gate
that does not fail after ten minutes. Stage 1 must decide from an Airflow 3
prototype whether this is best implemented as a dedicated sensor/state gate or
as explicit DAG pause/unpause with a manifest of the prior pause state. The
acceptance contract is fixed regardless of mechanism:

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

No production maintenance mode is declared in this stage.

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
the public site down until someone runs `up -d` by hand, and nothing pages for
it. The one-line fix belongs to Stage 2; Stage 0 only records it, and the test
asserts the registry keeps saying so for as long as the gap exists.

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

### Stage 1 — Add maintenance intent and truthful drain status

1. Add a maintenance state record/API distinct from deploy intent and make the
   two modes mutually exclusive.
2. Replace the ambiguous aggregate with named active-work counts. Preserve the
   existing deploy endpoint for compatibility, but stop presenting pending
   backlog as running work in the maintenance UI/API.
3. Add the maintenance-aware DAG gate selected in Stage 0. A maintenance pause
   reschedules or pauses safely without the deploy sensor's 600-second failure.
   If Stage 0 selects the pool, this is where the `solver` pool Plan 136 Stage
   3d expects comes into existence — name it and size it here, so the two plans
   share one pool rather than each declaring their own.
4. Add `request`, `mark-drained`, `mark-offline`, `begin-validation`, `cancel`,
   and explicit `complete` operations with legal-transition tests.
5. Add stale-maintenance metrics/alerts. Stale means “needs a human,” never
   “safe to resume.”

Verify in a non-outage dry run: request maintenance, prove new work is gated,
let current work drain, cancel, and prove normal schedules resume without failed
or duplicated mutations.

### Stage 2 — Build the checked-in host procedure

Add an operator-run script, proposed as `scripts/host_maintenance.sh`, with
idempotent subcommands rather than one irreversible monolith:

```text
preflight -> request -> wait-drained -> stop -> update -> reboot
          -> validate-host -> start -> validate-stack -> complete
```

The script prints and checkpoints every phase. It never stores credentials and
never calls `complete` from a general exit trap.

#### Preflight and package preparation

- verify SSH plus Oracle Cloud console access before risking network changes;
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
- **Boot failure:** use the Oracle Cloud console and previous kernel from GRUB.
- **Docker incompatibility:** restore the reviewed Docker/containerd package
  versions and daemon configuration, then validate Compose before starting.
- **Application regression:** keep maintenance active, return to the prior Git
  revision/images, and recreate only the affected services.
- **Mount/storage mismatch:** do not start stateful services; diagnose from the
  host/console with the manifest and `fstab` evidence.

No rollback path includes `git reset --hard`, deleting Docker volumes, `docker
volume prune`, or automatically releasing maintenance intent.

## Tests and CI invariants

1. Maintenance and deploy intent cannot coexist.
2. Every state transition accepts only its legal predecessor and is idempotent
   where operator retry is expected.
3. Pending backlog does not block drain; processing/running claims do.
4. A long maintenance pause does not fail DAGs merely because time passes.
5. The running-set manifest round-trips default, profile-gated, auxiliary, and
   intentionally stopped services.
6. Script dry-run tests assert phase ordering and prove no failure path calls
   `complete` implicitly.
7. Plan 140 coverage is a declared dependency of the resume gate; missing
   health data fails closed.
8. Package/reboot commands require explicit non-dry-run confirmation.

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
136's Stage 3d inherits it for the narrower solver case.

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

1. A separate maintenance state survives stack stop and reboot and never
   releases itself.
2. New mutating DAG work pauses without timeout failures; active work drains to
   named zero counts that exclude pending backlog.
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
