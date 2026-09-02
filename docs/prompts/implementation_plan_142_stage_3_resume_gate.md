# Implementation Plan 142 Stage 3: Make Plan 140 the Resume Gate

## Status

**Not started, written 2026-08-25** on branch
`feature/plan-142-stage-3-resume-gate`, after PR #251 landed Stage 2.

This is a working document: it breaks
[Stage 3](../plans/plan_142_planned_host_maintenance.md#stage-3--make-plan-140-the-resume-gate)
into reviewable commit slices and records the decisions taken before any code
was written. The design prose and as-built evidence stay in
[plan_142_planned_host_maintenance.md](../plans/plan_142_planned_host_maintenance.md);
this file is deleted or archived when Stage 3 closes.

## What Stage 3 owes

Stage 2 built the whole procedure through `begin-validation` and stopped there
deliberately. Three things are owed and one is a debt Stage 2 knowingly
incurred:

| Owed | Where it is stated |
|---|---|
| Host validation gates — kernel, mounts, network, Docker, apt/dpkg | Stage 3 items 1–5 |
| The stack gate — Plan 140 health, direct readiness, auxiliary-still-stopped | Stage 3, "The stack gate requires" |
| `complete`, guarded by validation evidence | `coordination.py:472`, durable-history item 5 |
| **Restoring apt automation**, masked by Stage 2's `update` | Stage 2: "Apt automation intentionally remains masked until the Stage 3 resume gate has passed" |

That last row is the one most likely to be forgotten, because nothing fails
when it is: the host simply stops receiving automatic security updates, quietly,
for as long as nobody notices. It gets its own slice.

## Decision — the gate is split, and each half is enforced where it can be seen

**The script runs the host gates; ops runs the stack gates; `complete` requires
both.** The alternative — the script runs everything and tells ops it passed —
was rejected because it makes the guard ceremonial. Ops can independently
observe the stack, so it must; an operator should not be able to hand it a
passing verdict for the half it can check itself.

| Half | Evaluated by | Why there |
|---|---|---|
| Host — kernel, mounts, systemd units, apt/dpkg, Docker daemon config | `scripts/host_maintenance.py` | ops runs *in a container*. It cannot see the running kernel, `/mnt/data`'s backing device, or `systemctl --failed` |
| Stack — Plan 140 health, service readiness, auxiliary-still-stopped, coordination state | `ops/routers/coordination.py` + a new `ops/coordination_release.py` | ops already reaches Postgres, container-health, and every service's `/ready`. Re-evaluated server-side at `complete` time, not trusted from a payload |

**The residual, stated plainly:** host evidence is operator-attested. Ops records
who submitted it, for which `generation`, and at what time, but cannot verify
that the kernel really booted. That is an honest limit of running the control
plane inside the thing being validated, not something to paper over — and it is
why the host evidence is durable and attributable rather than a boolean.

### Reuse the drain registry's shape, not a second mechanism

`ops/coordination_drain.py` already solved this exact problem for the drain
contract: a registry of named sources, each returning `known`/`unknown` with a
reason, and `unknown` failing closed. Stage 3's gates are the same shape and
should be the same code pattern — `RELEASE_GATES`, `collect_release_status()`,
one entry per gate.

Two Stage 1 lessons carry directly:

- **Defect 1** — three drain queries named the wrong schema, every one returned
  `unknown`, and the operator saw "still draining" rather than "your SQL is
  wrong". *A fail-closed gate that cannot distinguish failing from unreadable
  presents a defect as patience.* Every gate here reports `unknown` with a
  reason string, and the CLI prints the reason.
- **Defect 2** — a drain source whose evidence endpoint shipped in the same
  release hung the deploy that installed it. **Any gate in this stage that needs
  a new service endpoint must ship the endpoint in an earlier slice, deployed,
  before the gate that reads it is enabled.** Slice ordering below honours this.

## Unit tests assert code logic, not file-path mechanics

Development happens on macOS **and** Windows; CI runs Linux only
(`.github/workflows/ci.yml:108`). Tests written on one machine have repeatedly
come back red on another, most recently on the Stage 2 branch.

**The platform difference is the detector, not the defect.** Every one of the
repairs below was a test that reached out and touched the machine when it should
have been exercising a function. `0bee614`'s own commit message states the rule
this stage adopts:

> Each asserted a property of the host instead of the behavior the code owns, so
> each is corrected at the boundary where the assumption actually entered.

That is the whole contract. A unit test that can fail because of the operating
system it runs on is testing the operating system.

### What the six repairs were actually testing

| Commit | What it asserted | What it meant to prove |
|---|---|---|
| `820c944` | `str(args.sample_in) == "/tmp/s.json"` | that the flag parsed to the right path |
| `21333ab` | that `symlink_to` produced a real symlink | that we refuse to traverse a linked run directory |
| `43032b4` | Prometheus counters read while `builtins.open` was patched | that a dummy scrape is not counted |
| `0bee614` | repo source decoded with the locale codec | that two modules have not drifted apart |
| `0bee614` | the real `os.uname().release` | that the checkpoint record carries five fields |
| `0bee614` | the `0o755`/`0o644` bits the filesystem stored | that the client requests those modes |
| `0bee614` | the literal bytes `b"true\n"` | that the classifier said yes |
| `f49aeff` | — (import-time `LOG_PATH` default) | nothing; it was bootstrap, not a test |
| `c1c28de` | — (`.sh` checked out CRLF) | nothing; a real repo bug, already fixed by `.gitattributes` |

**Read the two columns against each other.** Not one of those tests set out to
test a filesystem — they were checking an argparse flag, a refusal to traverse,
a record's field set, a classifier's verdict. In every case the right-hand
column is provable with no I/O whatsoever. The filesystem got into the
assertion by accident, and the platform merely sent the bill.

`43032b4` is the clearest case, because it inverts. It failed on *Linux* and
passed on *Windows* — the test patched `builtins.open` and then walked the
Prometheus registry, whose `ProcessCollector` reads `/proc/<pid>/stat`. Windows
has no `/proc`, so the collector returned early and the defect hid. **A test
whose result depends on which kernel is underneath is not a portability
problem. It is a test with an undeclared dependency**, and it will keep finding
new ways to be wrong.

### The rules

1. **Assert the contract the code owns.** What did the flag parse to; what did
   the function decide; what fields does the record carry. Not what the OS did
   with it afterward.
2. **A unit test does no real I/O.** No real file read of repository source, no
   real subprocess, no real symlink, no real permission bit. If a test needs the
   filesystem to prove its point, the point belongs in an integration test.
3. **Push host mechanics behind a thin seam and test the logic, not the seam.**
   The seam is a handful of lines with no branching — `_run_command`
   (`host_maintenance.py:177`) is the model. Logic above it takes plain data and
   returns plain data.
4. **Data in, verdict out.** A gate is a pure function over captured evidence.
   Its test is a dict and an expected result, with no `tmp_path` at all.
5. **When a test must write a file, assert the call, not the consequence.**
   `0bee614`'s rule for `chmod`, with the precedent at
   `tests/scripts/test_host_maintenance.py:48`. The client owns which mode it
   requests; it does not own whether the filesystem honours it.
6. **Compare `Path` to `Path`**, and name the encoding on any text read.

### The design consequence, which is the actual point

**Stage 3 is the most host-coupled work in Plan 142** — kernel, mounts, systemd,
apt, Docker config. Written the obvious way, every gate test would need a mocked
filesystem and would be exactly the kind of test that has been breaking.

So the architecture has to carry the rule rather than the discipline:

| Layer | Does | Tested with |
|---|---|---|
| Collectors | Run one command or read one path; return plain dicts. No branching, no interpretation | Barely — they have no logic to get wrong |
| Gates | Pure functions: evidence dict in, pass/fail/`unknown` + reason out | Dicts. No `tmp_path`, no mocks, no platform |
| Command wiring | Calls collectors, feeds gates, writes evidence, sets exit code | The seam mocked once, asserting orchestration |

The gates hold every decision in this stage and become the easiest things in the
repo to test — a table of inputs and expected verdicts. **Portability stops
being a concern because nothing in the gate layer can tell what OS it is on.**
That is worth doing on test-quality grounds alone; not breaking on Windows is
the side effect.

### Why not a Windows CI leg

It was considered and rejected here. It would catch these late instead of
preventing them, and it legitimises the coupling: a test that needs two runners
to be trusted is a test asserting something it should not assert. Slower CI to
keep a defect observable is the wrong trade when the defect can be designed out.

Worth revisiting only if a genuine platform-specific *behaviour* ever needs
proving — none of the nine rows is that.

---

## The slices

Seven commits. Each one is independently reviewable, leaves the tree green, and
leaves production working — nothing here is deployed mid-stage except as noted.

**After every slice, invoke the commit skill** — `Skill(commit-plan-attribution)`
— rather than calling `git commit` directly. The skill requires explicit
attribution in the message; every commit in this stage carries `Plan 142 Stage 3.`
in its body. Suggested subjects are given per slice.

### Slice 1 — Collect host facts as data

**Foundation. It establishes the layering every later slice is written into.**

Add a `host_facts` collector that gathers what Stage 3 needs — kernel release,
boot id, mounts, disk headroom, unit states, Docker config, package state — and
returns **one plain dict**. All POSIX specifics live inside it; nothing above it
touches the machine.

Fold the two stragglers into it. `_running_kernel()` (`host_maintenance.py:956`)
is the instructive one: `checkpoint_record()` calls it, so a test that only
wants to assert *the record has five fields* currently has to mock the host to
do it. That is the coupling this stage is removing, and it is why every existing
checkpoint test carries a `_running_kernel` patch it should never have needed.
`_boot_id()` (`host_maintenance.py:960`) joins it for the same reason.

Once facts are a dict, `checkpoint_record()` takes the facts it needs as an
argument. Its test becomes a dict and an assertion, with no patching at all.

- **Files:** `scripts/host_maintenance.py`, `tests/scripts/test_host_maintenance.py`
- **Tests:** `checkpoint_record` tested as a pure function over supplied facts —
  and the `_running_kernel`/`_git_revision` patches deleted from those tests, not
  rewritten. The collector itself gets one test that it raises `MaintenanceError`
  on an unreadable host, with the seam mocked
- **Commit:** `refactor(plan-142): collect host facts as data`

### Slice 2 — Host validation gates and `validate-host`

Implement Stage 3 items 1–5 as a registry of **pure functions over Slice 1's
facts dict**, mirroring `DRAIN_SOURCES`. Signature is
`gate(facts, preflight) -> (verdict, reason)`; verdict is pass / fail /
`unknown`, and `unknown` is never a pass.

| Gate | Compares | Fails when |
|---|---|---|
| `kernel_expected` | `facts["kernel"]` vs the `updated` checkpoint's target | running kernel is not the one the package plan installed |
| `no_reboot_required` | `facts["reboot_required"]` | a reboot is still pending and was not expected |
| `mounts_expected` | `facts["mounts"]` vs the preflight bundle | `/` or `/mnt/data` missing, or backing device changed |
| `disk_headroom` | `facts["disk"]` | bytes or inodes below the reviewed floor |
| `host_services` | `facts["units"]`, DNS, clock, sshd | any required unit failed, or clock is not synchronised |
| `docker_daemon` | `facts["docker"]` vs the preflight bundle | log limits or storage path drifted |
| `package_state` | `facts["dpkg"]`, `facts["apt_locks"]` | an unfinished transaction or failed configuration remains |

`mounts_expected` and `docker_daemon` compare against the **preflight evidence
bundle** captured before the window — that comparison is what makes them
meaningful rather than static assertions, and it is also why both sides of every
gate are already plain data.

`validate-host` is the wiring: collect facts, load the preflight bundle, run the
registry, write `validate-host.json`, exit non-zero on any fail or `unknown`. It
writes **no checkpoint** — Stage 2 established that read commands never
checkpoint (`test_read_commands_never_write_checkpoint`).

- **Files:** `scripts/host_maintenance.py`, `tests/scripts/test_host_maintenance.py`
- **Tests:** one parametrized table — facts dict in, expected verdict and reason
  out — covering pass, fail and `unknown` for all seven gates, plus registry
  fail-closed on a single `unknown`. **No `tmp_path`, no mocks, no subprocess in
  any gate test.** The wiring gets its own small test with the collector mocked
- **Commit:** `feat(plan-142): add host validation gates`

### Slice 3 — Stack release gates in ops

New `ops/coordination_release.py`, shaped like `coordination_drain.py`, plus
`GET /coordination/release-status`.

| Gate | Source | Fails when |
|---|---|---|
| `expected_services_present` | container-health + the running-set manifest | a service the manifest says should be running is absent |
| `container_health` | Plan 140's three-state metric | any in-scope service unhealthy **or unconfigured**; absence is not a pass |
| `service_readiness` | direct `/ready` on Postgres, MinIO, Airflow, ops, Prometheus, Grafana, Loki, Promtail | any probe fails or is unreachable |
| `observability_fresh` | Prometheus scrape recency, Loki ingestion | scrapes stale, or Promtail replay/error storm (Plan 141's contract) |
| `auxiliary_still_stopped` | container-health, keyed on Compose project | a Plan 125 auxiliary project came back up |
| `coordination_expected` | `coordination_state` | phase is not `validating`, or kind is not `host_maintenance` |

**`auxiliary_still_stopped` is this plan's own check and cannot be delegated to
Plan 140.** `health_values()` filters on `com.docker.compose.project` and drops
everything that is not `cartracker` — correctly, per its docstring and the Plan
140 Stage 1 soak. Plan 142 Stage 0 item 2, Finding 2 already established that
this gate is Plan 142's to implement. Read the sibling projects directly.

This slice ships the endpoint **without** wiring it into `complete`. That is
Defect 2's ordering rule: the endpoint deploys first, then the guard that
depends on it.

- **Files:** `ops/coordination_release.py`, `ops/routers/coordination.py`,
  `tests/ops/test_coordination_release.py`, `tests/ops/routers/test_coordination.py`
- **Tests:** each gate pass/fail/unknown; unknown never passes; auxiliary gate
  keyed on project, proven against a fixture with a running sibling; the
  endpoint returns the full gate list, not a bare boolean
- **Commit:** `feat(plan-142): add stack release gates`

### Slice 4 — Durable host evidence (V045)

Append-only `coordination_release_evidence`, following V043's precedent for
`coordination_gate_observations` — *"Historical rows are harmless and make the
proof auditable."* One row per submission: `generation`, actor, submitted-at,
gate results, and the evidence-bundle digests.

`POST /coordination/host-evidence` accepts the bundle Slice 2 writes, validates
that it covers every registered host gate and names the current `generation`,
and records it. It **does not** transition anything.

Register the table in `archiver/processors/flush_staging_events.py` if it is
placed in `staging`, matching how V044's events flush. Expand-only; reverts by
reverting the migration and the registry entry.

- **Files:** `db/migrations/V045__coordination_release_evidence.sql`,
  `ops/routers/coordination.py`, `archiver/processors/flush_staging_events.py`,
  `scripts/host_maintenance.py` (submit from `validate-host`), tests for each
- **Tests:** submission rejected when a gate is missing; rejected on a stale
  `generation`; accepted and readable back; a failed insert leaves no partial
  state
- **Commit:** `feat(plan-142): persist host validation evidence`

### Slice 5 — `complete`, guarded

The transition already exists — `_TRANSITIONS["complete"]` is defined at
`coordination.py:26` and unused. Only the guarded route is missing, and the
comment at `coordination.py:472` says why: *"Exposing an unguarded
validating->none endpoint would turn state into false authority."*

`POST /coordination/complete` requires, in one advisory-locked transaction:

1. phase is `validating` and kind is `host_maintenance`;
2. **every stack gate passes, re-evaluated now** — not read from a cache, not
   taken from the request body;
3. a host-evidence row exists for the current `generation` with every gate
   passing;
4. explicit operator confirmation in the payload.

Any failure returns `409` with the failing gates named, and **leaves the phase
at `validating`**. Stage 3's own words: "A failed gate leaves the system in
`validating`, with work paused."

Then the client half: a `complete` subcommand requiring `--confirm-complete`,
and **durable-history item 5** — add the completion phase to
`CHECKPOINT_PHASES` (`host_maintenance.py:24`) so the local breadcrumb records
the window closing. That set currently ends at `validating`, which is why item 5
was blocked here.

Update `STAGE2_PROCEDURE` (rename to the full procedure) so `plan` emits the
whole chain through `complete`. Stage 2's test asserting there is *no* complete
path is replaced by one asserting complete exists **and is unreachable without
both evidence halves**.

- **Files:** `ops/routers/coordination.py`, `scripts/host_maintenance.py`,
  `tests/ops/routers/test_coordination.py`, `tests/scripts/test_host_maintenance.py`
- **Tests:** complete refused with no host evidence; refused with a failing
  stack gate; refused from every wrong phase; refused without confirmation;
  succeeds with both halves and writes exactly one V044 event; the checkpoint
  records completion; `plan` emits the canonical order
- **Commit:** `feat(plan-142): guard release on validation evidence`

### Slice 6 — Restore apt automation after the gate

Stage 2's `update` masks `apt-daily.timer`, `apt-daily-upgrade.timer` and
`unattended-upgrades.service` (`APT_CONTROL_UNITS`, `host_maintenance.py:48`)
and deliberately leaves them masked. Stage 3 is where they come back.

A `restore-apt-automation` step that unmasks and re-enables each unit, restores
the enablement state `update` recorded, and **verifies** it — then confirms the
reviewed hold set is still intact.

**Ordering is load-bearing and was learned the hard way.** Plan 142 Stage 0 item
4: the August window restored the timers in the same command that refreshed and
simulated, and `unattended-upgrades` started three minutes later, *inside the
window*. Restoration happens **after** the resume gate passes, never before.

- **Files:** `scripts/host_maintenance.py`, `tests/scripts/test_host_maintenance.py`
- **Tests:** refuses to run before the gate has passed; unmasks exactly the
  recorded units; verifies enablement rather than assuming it; fails closed if a
  unit does not come back; holds still intact
- **Commit:** `feat(plan-142): restore apt automation after the resume gate`

### Slice 7 — Runbook and plan document

The operator sequence for validation and release, added to
`docs/runbooks/runbook_host_maintenance.md` in the style of §10 — blast radius
and abort per step. Then the as-built section in
`plan_142_planned_host_maintenance.md`, and the CI-invariant checkboxes (7 and
12) marked.

**`docs/PLANS.md` is not edited by hand here.** Moving Plan 142's row is a state
transition — run the `plans` skill, with the gate and next slice decided in the
open first.

- **Files:** `docs/runbooks/runbook_host_maintenance.md`,
  `docs/plans/plan_142_planned_host_maintenance.md`
- **Commit:** `docs(plan-142): document the Stage 3 resume gate`

---

## Non-Goals

- **Executing a production host window.** That is Stage 4, and it additionally
  waits on Plan 136 Stage 3b's soak closing 2026-08-27.
- **A Windows CI runner.** Rejected above: it would catch the coupling late
  rather than removing it.
- **Retrofitting the rest of the suite.** The rules apply to what this stage
  writes and to tests it already touches. A repo-wide audit is separate work and
  should be its own plan if it is ever worth doing.
- **`.env.example`'s twelve undocumented variables.** Found during Stage 0 item
  6 and explicitly owed to neither that item nor this stage.
- **Changing Plan 140's project filter.** Its docstring records why it must not
  be relaxed; the auxiliary gate reads siblings directly instead.

## Done when

1. All seven slices are merged, and **no test added by this stage touches the
   filesystem, a subprocess, or a permission bit to make its point.** The gate
   tests are tables of data; that is the check, and it is readable in the diff
   rather than dependent on which machine ran it.
2. `complete` cannot be reached without a passing stack gate re-evaluated
   server-side and a durable host-evidence row for the current generation.
3. Durable-history item 5 is checked, so the coordination lifecycle terminates
   in both Postgres and the local checkpoint.
4. Apt automation is restored and verified, and the plan says where.
5. A completed window is reconstructable from Postgres after its `generation`
   has been superseded — CI invariant 12, now actually satisfiable.
