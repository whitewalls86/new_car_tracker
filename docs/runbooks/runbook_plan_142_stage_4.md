# Runbook: Plan 142 Stage 4 — the first reviewed host-maintenance window

**Status: DRAFT, for review. Not yet run.** This is the run sheet for
[Plan 142](../plans/plan_142_planned_host_maintenance.md) Stage 4 — one reviewed
security/package maintenance window executed through the checked-in procedure in
[`scripts/host_maintenance.py`](../../scripts/host_maintenance.py), with the
handoff evidence Stage 4 asks for.

It extends [runbook_host_maintenance.md](runbook_host_maintenance.md); that file
stays the reference for *why* each check exists and for the 2026-08-18 window it
was recovered from. This file is the ordered sheet for *this* window, against the
host as measured on 2026-08-29.

Everything in §4 was read from production on 2026-08-29 (23:20–23:55 UTC) with
read-only commands.

---

## 1. Five things block this window

Four are defects found while scoping it. None is optional. B2 and B5 strand the
window *after* the reboot, with production stopped and coordination unable to
release.

### B1 — The Stage 3 resume gate is not deployed. Two images are stale.

The running `cartracker-ops` image was built **2026-08-25T19:45:26Z**. Its
OpenAPI publishes:

```
/coordination/{request,begin-drain,authorize,begin-validation,cancel,status,drain-status,local-drain}
/deploy/{start,status,complete}   /admin/deploy{,/start,/complete}
```

There is **no `/coordination/release-status`, no `/coordination/host-evidence`,
no `/coordination/complete`.** Those three are Stage 3 (PR #251, merged
2026-08-26) and live in `ops/routers/coordination.py`. Five `ops/` commits sit
between the deployed image and `master`, four of them Stage 3.

The migrations are already ahead of the code: V044, V045 (release evidence) and
V046 (completion receipts) applied 2026-08-27, V047 on 2026-08-28. Expand-only,
so the split is harmless at rest — but it means the tables exist and the code
that writes them does not.

Consequence if the window runs as-is: `validate-host` POSTs its bundle to
`/coordination/host-evidence` → 404; `complete` POSTs `/coordination/complete` →
404. The window strands in `validating` with DAGs gated, and the only release
route deployed is the legacy `/deploy/complete` facade — which
[§10.7](runbook_host_maintenance.md#107-stage-3-resume-gate--host-maintenance-only)
explicitly forbids.

**It is not only `ops`.** The same commit that added the stack gates
(`4d6ed4a`) added `GET /project-status/{project}` to
[`container_health/app.py`](../../container_health/app.py), and
`_auxiliary_still_stopped` calls it once per auxiliary project. The deployed
`cartracker-container-health` image is also from **2026-08-25**, and the
endpoint 404s live:

```
$ docker exec cartracker-ops python -c '...urlopen...'
200 http://container-health:9110/health                            {"ok":true}
ERR http://container-health:9110/project-status/cartracker-mlflow  HTTPError 404
```

A 404 returns `{"detail":"Not Found"}`, so `response.json()` succeeds,
`payload.get("known")` is not `True`, and the gate raises → `_unknown` → blocker.
`unknown` never grants release. So with `ops` current and `container-health`
stale, the resume gate still could not pass.

**Action:** deploy `master`, rebuilding **both `ops` and `container-health`**,
before the window, as an ordinary scoped deploy through `scripts/redeploy.sh`.

Two things make this a plain deploy rather than a special case:

- **No config change is needed.** `ops/Dockerfile` is `COPY . .`, so the image
  already carries the `container_health` package that `coordination_release.py`
  imports. `CONTAINER_HEALTH_URL`, `PROMETHEUS_URL` and `LOKI_URL` all default to
  Compose service names, and all three resolve from `ops` on `cartracker-net`
  today (`container-health:9110`, `prometheus:9090/-/ready`, `loki:3100/ready`
  all answer 200). The migrations this code wants — V044–V047 — are already
  applied.
- **`ops` redeploying itself is fine.** Coordination state lives in Postgres and
  survives container replacement by design, and `redeploy.sh` releases through
  the `/deploy/complete` compatibility facade, which exists in both the old and
  the new image. Watch the gap while the container restarts; nothing else.

Then re-read `/coordination/release-status` and require a 200 before scheduling.

### B2 — `validate-host` will hard-fail: this transaction has no kernel package

`apply_package_plan` derives the boot target only from the packages *in the
transaction*:

```python
boot_kernel_targets = sorted(
    name.removeprefix("linux-image-") for name in installed_versions
    if name.startswith("linux-image-") and name not in {...generic...}
)
boot_kernel_target = boot_kernel_targets[-1] if boot_kernel_targets else None
```
— [`scripts/host_maintenance.py:1236`](../../scripts/host_maintenance.py)

This window's pending transaction contains **no `linux-image-*`**:
`unattended-upgrades` already installed `6.8.0-1060-oracle` days ago, and the
host simply has not rebooted. So `boot_kernel_target` is `None`, `kernel_target`
is never written to the `updated` checkpoint, and then:

```python
expected_kernel = updated.get("kernel_target")
if not isinstance(expected_kernel, str) or not expected_kernel:
    raise MaintenanceError("updated checkpoint has no kernel target")
```
— [`scripts/host_maintenance.py:1404`](../../scripts/host_maintenance.py)

`validate-host` runs **after `reboot` and after `start`**. So the failure lands
at the end of the window: host rebooted, stack back up, coordination pinned at
`validating`, and no path to `complete` because `complete` requires the durable
host-evidence row.

This is not a corner case — it is the *normal* shape of a window on this host,
because unattended-upgrades installs kernels and only the reboot is deferred.
Stage 2 assumed the window that installs the kernel is the window that boots it.

**Action:** fix before the window. Fall back to the highest installed versioned
`linux-image-*-oracle` on the host when the transaction carries no kernel
package, and record in the checkpoint which of the two sources the target came
from. `GRUB_DEFAULT=0` with `GRUB_TIMEOUT=0`, so the highest installed kernel is
what boots — here `6.8.0-1060-oracle` (`/boot` holds `1058` and `1060`;
`reboot-required.pkgs` also names `1059`, which has since been autoremoved).

### B3 — A `docker compose run` one-off makes `stop` fail closed

`capture_running_set` walks `docker ps --all` and records every container with no
`com.docker.compose.oneoff` filter. `build_running_set_plan` then rejects two
running containers sharing one `(project, service)` identity:

```python
if identity in seen:
    raise MaintenanceError(f"running-set manifest duplicates {project}/{service}")
```
— [`scripts/host_maintenance.py:341`](../../scripts/host_maintenance.py)

As of 2026-08-29 23:30 UTC there are **three** running containers with identity
`cartracker/archiver`: the service itself, plus two `oneoff=True` run containers
—the Plan 145 `delete_packed_source_html --apply`, and
`cartracker-archiver-run-44d23542ea2c`, an ad-hoc `python -c` lake query that has
been **up 8 hours** and looks leaked.

`preflight` does not derive the plan, so it captures the duplicate happily and
the refusal surfaces at `stop` — after `request`, `drain` and `wait-active`, with
production already gated.

Plan 140 already learned this: `container_health/collector.py:149` skips
`ONEOFF_LABEL`, and `oneoff_processes()` treats one-offs as drain evidence,
never as expected services. Plan 142's capture never got the same treatment.

**Action, minimum:** no one-off may be alive when `preflight` or `stop` runs.
Clean up the leaked 8-hour container first. **Action, real fix:** skip
`oneoff=True` in `capture_running_set`, or fail in `preflight` where it is free
to abort.

### B5 — `oauth2-proxy` fails the `container_health` release gate, permanently

This one is not fixed by any rebuild.

`_container_health` fails if **any** expected service reads anything but `1`:

```python
bad = sorted(service for service in EXPECTED_SERVICES if values.get(service) != 1)
```
— [`ops/coordination_release.py:111`](../../ops/coordination_release.py)

`oauth2-proxy` is in `EXPECTED_SERVICES`, and it reads **`-1`** — unconfigured —
right now and has since 2026-08-20:

```
{'__name__': 'cartracker_container_health', 'container': 'oauth2-proxy', ...} -1
```

That is not drift. It is Plan 140's one documented unresolved service: the
distroless `quay.io/oauth2-proxy/oauth2-proxy:latest` has no shell or HTTP
client for Docker to exec, so no healthcheck can be configured. It is recorded
in [`healthcheck-exemptions.txt`](../../healthcheck-exemptions.txt), and
`ct-container-health-unconfigured` has been alerting on it daily since
2026-08-20 by design.

**Nothing in `ops/` or `container_health/` reads that exemptions file** — it is
consumed only by the tests, the deploy script and `shared/log_ingestion_policy.py`.
So the gate has no exemption path, and `container_health` will report
`fail: unhealthy, unconfigured, or absent: oauth2-proxy` on **every** host
window, forever.

Plan 142 Stage 3 does say the gate must show "neither unhealthy nor unconfigured
services hidden as absence" — so `-1` failing is deliberate. What was missed is
that one expected service is permanently `-1` on purpose, which turns a
fail-closed gate into a gate that cannot open.

**Action, pick one before the window:**

- teach the release gate the documented exemption — treat an exempt service's
  `-1` as pass, and fail if a *non*-exempt service reads `-1`. This keeps the
  contract and is the smaller change; or
- give `oauth2-proxy` a real healthcheck by moving the front door to the Alpine
  variant. Plan 140 already scopes this as a separate front-door image change,
  and it should not ride inside the first maintenance window.

Either way, verify by reading `/coordination/release-status` and seeing
`container_health: pass` **before** anything is stopped. This gate is readable
at any time, costs nothing, and is the cheapest possible dress rehearsal for
the whole resume path.

### B4 — Plan 145 Stage 6 must be finished first

The Stage 6 deletes are running right now
(`delete_packed_source_html --year 2026 --month 4 --apply`, then `delete-legacy`
for the 1,172 objects). V042 pauses long jobs on deploy intent, and the Stage 6
sheet says not to trigger a deploy while its packing runs, because resuming is
what duplicates members.

**Gate:** no coordination request — for the B1 deploy or for the window — until
Stage 6 reports done and both `cartracker-archiver-run-*` containers have exited.

---

## 2. What this window actually applies

Two things, and they are worth separating because only one of them shows up in
`package-plan.json`:

1. **The overdue reboot.** Kernel `6.8.0-1060-oracle` has been installed and
   unbooted; the host is running `6.8.0-1058-oracle`, up 11 days 19 h since
   2026-08-18 04:22 UTC. `/var/run/reboot-required` is set. This is the real
   reason for the window.
2. **17 ordinary `jammy-updates` packages.** No security-origin package is
   pending — `unattended-upgrades` ran at 06:12 UTC this morning and installed
   the `libpam*` / `libp11-kit0` set. This window is housekeeping plus the
   reboot, not an emergency.

### The transaction, as `prepare-update` will resolve it

`prepare-update` uses `apt-get --simulate upgrade` (not `full-upgrade`), so this
is exactly 17 packages, **0 new, 0 removed**:

| Package(s) | To version | Boundary |
|---|---|---|
| `python3.10`, `python3.10-minimal`, `libpython3.10`, `libpython3.10-stdlib`, `libpython3.10-minimal` | `3.10.12-1~22.04.17` | — |
| `netplan.io`, `netplan-generator`, `libnetplan0`, `python3-netplan` | `0.107.1-3ubuntu0.22.04.4` | **network** |
| `iproute2` | `5.15.0-1ubuntu2.2` | — (not matched by `PACKAGE_BOUNDARIES`, but it is networking — review it as such) |
| `ubuntu-release-upgrader-core`, `python3-distupgrade` | `1:22.04.21` | — |
| `cloud-init` | `26.1-0ubuntu1~22.04.1` | — |
| `snapd` | `2.76.3+ubuntu22.04` | — |
| `qemu-user-static` | `1:6.2+dfsg-2ubuntu6.31` | — |
| `libjcat1` | `0.2.3-1~ubuntu0.22.04.1` | — |
| `libxmlb2` | `0.3.24-1~ubuntu0.22.04.1` | — |

Three things to hold in mind while reviewing it:

- **`--include-held docker.io` is mandatory** — `prepare-update` refuses unless
  every held package is named exactly. It does **not** upgrade docker.io:
  Installed equals Candidate at `29.1.3-0ubuntu3~22.04.2`, so the simulation
  adds nothing. The hold is intact and is the only hold, matching policy.
- **`requires_reboot_review` will be `false`** in `package-plan.json`, because no
  kernel package is in the transaction. Do not read that as "no reboot needed" —
  see B2. The reboot is the point of the window.
- **`full-upgrade` would take 5 more packages** (`fwupd` plus new `jq`,
  `libfwupd3`, `libjq1`, `libonig5`, `libprotobuf-c1`). `prepare-update`'s plain
  `upgrade` holds `fwupd` back. Keep it that way — no new packages in the first
  observed window.
- **netplan is the one real compatibility boundary.** `sshd -t` and
  `netplan generate` are the two checks that decide whether you can get back into
  a rebooted host; run both, as §2 of the parent runbook says.

---

## 3. Timing

VM and all timestamps here are UTC; you are US Central (UTC-5).

DAG schedules to stay clear of: `cleanup_parquet` 03:00, `disk_usage` 04:00,
`compact_silver` 04:10, `prune_task_logs` Sun 04:17, `pack_bronze_html` 06:00 on
the 3rd — plus the top of every hour (`hourly_analytics_refresh`,
`cleanup_artifacts`, `cleanup_queue`). The `*/5`, `*/15` and `*/30` DAGs are what
the drain gate is for and do not constrain the choice.

One more: `unattended-upgrades` runs around **06:12 UTC**. `update` masks the apt
units, but only at the `update` step — between `prepare-update` and `update` an
automatic transaction could invalidate the pinned plan. It would fail closed
(the plan re-simulates and verifies installed versions), but it costs the window.

**Recommended:** offline phase at **01:15 UTC = 20:15 CDT**, past the top of the
hour, clear of the 03:00–04:30 daily block and of 06:12.

| Phase | Planned | Basis |
|---|---|---|
| `preflight` + `prepare-update` | 10 min, run earlier the same day | read-only + download-only |
| `request` → `drain` → `wait-active` | 2–5 min | Stage 0 Phase B drained 44 tasks in 74.5 s |
| `stop` | 1–2 min | 30 containers, `stop` not `down` |
| `update` | 2–4 min | 17 packages, pre-downloaded |
| `reboot` → back | 1–2 min | host returned in 35 s on 2026-08-18 |
| `start` | 2 min | manifest-scoped, profiles carried |
| `begin-validation` → `validate-host` → release-status | 5 min | seven host gates + six stack gates |
| `complete` + `restore-apt-automation` | 2 min | |
| **Total** | **~25–35 min**, of which **~5 user-visible** | 2026-08-18 ran 03:59→04:26 with ~3 min visible |

---

## 4. Host baseline, measured 2026-08-29

Compare against this on the way out; it is also the pre-window half of the
Stage 4 before/after record.

| | Value |
|---|---|
| OS | Ubuntu 22.04.5 LTS |
| Kernel (running) | `6.8.0-1058-oracle`, booted 2026-08-18 04:22:27 |
| Kernel (installed) | `6.8.0-1058-oracle`, `6.8.0-1060-oracle` |
| `reboot-required` | **set** — names `linux-image-6.8.0-1059-oracle`, `linux-base`, `linux-image-6.8.0-1060-oracle` |
| apt index | fresh (2026-08-29 00:05 UTC) |
| apt/dpkg locks | none; `dpkg --audit` clean |
| Holds | `docker.io` only — correct |
| apt units | `apt-daily.timer`, `apt-daily-upgrade.timer`, `unattended-upgrades.service` all enabled + active |
| Failed systemd units | none |
| Docker | 29.1.3, Compose 2.40.3 |
| `/` | 49 G, 35 G used, **14 G free, 72%** — 11% inodes |
| `/mnt/data` | 196 G, 83 G used, 104 G free, 45% — 32% inodes |
| Containers | 30 running (+2 one-off, see B3) |
| Coordination | `phase=none`, `generation=16`, released 2026-08-29 14:53 UTC |
| `maintenance` pool | 16 slots ("released 2026-08-25") — correct |
| Checkout | `master` @ `64631de`, 3 untracked (`.env.bak-*` ×2, `ck_apr_detail.json`) |

**Watch item — `/` headroom.** `validate-host`'s `disk_headroom` gate floors `/`
and `/mnt/data` at **10 GiB available**. `/` has 14 G. That is 4 G of margin, and
`/` was **49%** right after the 2026-08-18 window — it has gained ~23 points in
11 days.

The growth is **`/var/lib/containerd` at 29 GB**. `/var/lib/docker` is only
714 MB, and that is the path
[runbook_storage_maintenance.md](runbook_storage_maintenance.md) measures — so
its "0 dangling images" reading is true and completely misses this. Docker 29
keeps image content in the containerd store; repeated builds accumulate there.
`docker system df` does not return within 100 s on this host, which is its own
signal.

Do **not** prune blind: rollback depends on the previous images being present,
and `docker system prune -a` is on the storage runbook's "be careful" list.
Measure it before the window; if headroom is wanted, `docker builder prune`
first. Either way it wants its own slice, not this one.

**Checkpoint file.** The tail of `/var/lib/cartracker/maintenance/history.jsonl`
is still `phase=preflight` from the PR #251 dry run of 2026-08-26, pointing at
`pr-251-preflight-20260826/running-set.json`. `stop`/`start` read the *latest*
entry, so this is harmless as long as this window's `preflight` is appended
before `request`. Do not reuse the old evidence directory.

---

## 5. The window

`EVIDENCE` is immutable for the whole window and is what every later command
matches against.

```bash
export EVIDENCE=/var/lib/cartracker/maintenance/stage4-$(date -u +%Y%m%dT%H%M%SZ)
export MANIFEST="$EVIDENCE/running-set.json"
export PREFLIGHT="$EVIDENCE/preflight.json"
export PLAN="$EVIDENCE/package-plan.json"
export API=http://localhost:8060
sudo install -d -m 0755 "$EVIDENCE"
```

> **Pass `--api-url $API` on every command.** The script defaults to
> `http://localhost:5050`, which on this host answers **500**. The coordination
> API is on **8060** — `redeploy.sh` has it right (`OPS_URL="http://localhost:8060"`),
> and §10.7 of the parent runbook omits the flag and would fail as written.
> That omission should be fixed in §10.7 too.

### 5.0 Gate — before anything

```bash
# Plan 145 Stage 6 done, and no one-off alive (B3, B4)
docker ps --filter name=archiver-run --format '{{.Names}} {{.Status}}'   # expect: empty
# Stage 3 resume gate deployed (B1) AND every gate readable and passing
#   except coordination_expected, which is correctly false outside a window (B5).
curl -sf "$API/coordination/release-status" | python3 -m json.tool
# Coordination clean
curl -sS "$API/coordination/status"     # expect phase=none
docker exec cartracker-airflow-scheduler airflow pools list   # maintenance = 16
```

Any of those failing ends the attempt here, with nothing changed. In
particular `container_health` must read `pass` — if it names `oauth2-proxy`,
that is B5 and the window cannot complete.

### 5.1 Preflight — read-only, safe days ahead

```bash
python scripts/host_maintenance.py --api-url "$API" --manifest "$MANIFEST" \
  preflight --output-dir "$EVIDENCE"
```

Fails closed on apt/dpkg locks, package-database inconsistency, or hold drift.
Record `preflight.json` and `running-set.json` SHA-256 and the container count
(expect **30**, and expect the three sanitized Compose records: `cartracker`,
`cartracker-lakehouse`, `cartracker-mlflow`).

Then the two lockout checks by hand — they are not in the script:

```bash
sudo sshd -t && echo sshd-valid
sudo netplan generate && echo netplan-valid   # warns about /run/netplan/enp0s6.yaml perms; baseline, not a symptom
findmnt --verify --verbose | tail -5          # three warnings are this host's baseline
```

### 5.2 Prepare the package plan — downloads, installs nothing

```bash
python scripts/host_maintenance.py --api-url "$API" --manifest "$MANIFEST" \
  prepare-update --output-dir "$EVIDENCE" --include-held docker.io
```

Review `package-plan.json` against §2 above, then record the printed
`package_plan_sha256` — `update` and `restore-apt-automation` both require it:

```bash
export PLAN_SHA=<the printed digest>
```

**Abort if** the package set differs from §2, anything appears under
`compatibility_boundaries` other than `network`, or any package is removed.

### 5.3 Request, drain, authorize

```bash
python scripts/host_maintenance.py --api-url "$API" --manifest "$MANIFEST" request \
  --requested-by "<operator>" \
  --reason "Plan 142 Stage 4: reboot to 6.8.0-1060-oracle + 17 jammy-updates packages" \
  --expected-work "17 packages, no new, no removals" \
  --expected-work "reboot to 6.8.0-1060-oracle"

python scripts/host_maintenance.py --api-url "$API" --manifest "$MANIFEST" drain
python scripts/host_maintenance.py --api-url "$API" --manifest "$MANIFEST" wait-active
```

`wait-active` polls every 5 s with no deadline and prints drain progress at most
once a minute. **Capture its first and last progress lines** — the named
blockers and the time to zero are Stage 4 evidence.

**Abort:** cancel via `/coordination/cancel` and nothing has changed. If one
count sits non-zero, read its oldest-start timestamp rather than waiting blind;
pending artifacts are backlog and must not block.

### 5.4 Stop — first user-visible moment

```bash
python scripts/host_maintenance.py --api-url "$API" --manifest "$MANIFEST" stop
```

`stop`, not `down`. It stops only what the manifest recorded running, carries the
`trawl` profile, leaves `cartracker-lakehouse` and `cartracker-mlflow` stopped,
and appends `stopped` only after every container's Docker state confirms it.

**If it refuses with "running-set manifest duplicates":** that is B3. A one-off
started between preflight and now. Cancel the window; do not hand-edit the
manifest.

### 5.5 Update

```bash
python scripts/host_maintenance.py --api-url "$API" --manifest "$MANIFEST" update \
  --package-plan "$PLAN" --confirm-plan "$PLAN_SHA" \
  --confirm-apply --release-notes-reviewed --compatibility-reviewed
```

Masks the apt timers and `unattended-upgrades`, applies only the pinned argv,
verifies installed versions and `dpkg --audit`, proves the hold set survived,
syncs, writes `update-result.json`, checkpoints `updated`.

Apt automation stays masked until after `complete`. That is deliberate.

**Abort:** finish `dpkg` to a consistent state — never interrupt it. Roll back
from the reviewed cached versions.

### 5.6 Reboot

```bash
python scripts/host_maintenance.py --api-url "$API" --manifest "$MANIFEST" \
  reboot --confirm-reboot
```

Then reconnect and replay the same command; it records `rebooted` only after
observing a **different boot ID**. Command return is never reboot evidence.
Pre-reboot boot ID: `734c55e0-6379-4a60-a2de-bfb65d74f97d`.

**Expected after boot:** `uname -r` → `6.8.0-1060-oracle`, `/mnt/data` on
`/dev/sdb` (UUID `e20a83a2-dd74-47d3-9208-1d2243d68236`), `/` on `/dev/sda1`,
`dockerd --validate` → configuration OK.

**Abort:** provider console, select the previous kernel (`1058` is still in
`/boot`). Do not start stateful services onto an unverified mount.

### 5.7 Start and validate

```bash
python scripts/host_maintenance.py --api-url "$API" --manifest "$MANIFEST" start
python scripts/host_maintenance.py --api-url "$API" --manifest "$MANIFEST" begin-validation
python scripts/host_maintenance.py --api-url "$API" --manifest "$MANIFEST" validate-host \
  --preflight "$PREFLIGHT" --output-dir "$EVIDENCE"
curl -sf "$API/coordination/release-status"
```

`start` restores from the manifest, so `trawl` and `redis-trawl` come back with
their profile — the failure mode §7 of the parent runbook records (a plain
`up -d` silently skips them and every detail scrape fails) is what the manifest
path exists to prevent. Confirm it anyway.

`validate-host` evaluates seven gates: `kernel_expected`, `no_reboot_required`,
`mounts_expected`, `disk_headroom`, `host_services`, `docker_daemon`,
`package_state`. `release-status` independently reports six:
`expected_services_present`, `container_health`, `service_readiness`,
`observability_fresh`, `auxiliary_still_stopped`, `coordination_expected`.

**If `validate-host` raises "updated checkpoint has no kernel target", that is
B2 and it should have been fixed before the window.** Coordination stays at
`validating`; production stays paused.

**Abort on any `fail` or `unknown`:** leave it in `validating`, investigate the
named reason. Do not edit evidence or retry around a refusal.

### 5.8 Complete, then restore apt automation

```bash
python scripts/host_maintenance.py --api-url "$API" --manifest "$MANIFEST" \
  complete --confirm-complete

python scripts/host_maintenance.py --api-url "$API" --manifest "$MANIFEST" \
  restore-apt-automation --package-plan "$PLAN" --confirm-plan "$PLAN_SHA"
```

`complete` re-evaluates the stack gates server-side and requires the durable
host-evidence row. A **409 leaves the phase at `validating`** — fix the named
gate and repeat 5.7; never force it, and never use `/deploy/complete`.

Restoration must come **after** `complete`, never before. Verify the three units
are back to enabled + active and that `apt-mark showhold` is still exactly
`docker.io`.

---

## 6. Evidence Stage 4 requires

Collect into `$EVIDENCE` and write the result into
[plan_142](../plans/plan_142_planned_host_maintenance.md) — Stage 4 names all
seven:

1. **planned vs actual phase duration** — §3's table against the checkpoint
   timestamps in `history.jsonl` and `staging.coordination_state_events`;
2. **drain counts and time to zero** — from `wait-active`'s progress lines;
3. **packages and kernel before/after** — §4 against `update-result.json`;
4. **downtime and health convergence** — `stop` → `start`, and `start` → all
   gates passing;
5. **any manual command not represented by the script** — expect at least
   `sshd -t`, `netplan generate`, `findmnt --verify`, and the reconnect after
   reboot. Each becomes a script/runbook step or gets a written reason;
6. **alerts/notifications sent** — including whether stale-coordination alerted;
7. **explicit resume evidence and post-resume DAG behaviour** — `validate-host.json`,
   the `release-status` response, the completion response, restored unit states,
   and the first clean run of `scrape_detail_pages` and `orphan_checker`.

Also worth recording, since a completed window is meant to be reconstructable
from Postgres after its generation is superseded (CI invariant 12): the
`generation` this window used, and that `staging.coordination_state_events` holds
one row per transition for it.

---

## 7. Findings to file regardless of when the window runs

Each of these is a Stage 4 discovery about the Stage 2/3 machinery, not about the
host. They belong in the plan document, and B1–B3 want tickets.

| # | Finding | Fix |
|---|---|---|
| 1 | Deployed `ops` **and** `container-health` predate the Stage 3 resume gate while its migrations are applied | Deploy `master` rebuilding both; consider a gate that refuses a host window when the API lacks the release routes |
| 1b | The `container_health` release gate fails permanently on `oauth2-proxy`'s documented `-1`; no runtime code reads `healthcheck-exemptions.txt` | Honour the exemption in the gate, or fix the front-door image (Plan 140 scopes it separately) |
| 2 | `boot_kernel_target` only ever comes from the current transaction, so a deferred-reboot window strands at `validate-host` | Fall back to the highest installed versioned kernel; record the source |
| 3 | `capture_running_set` has no `oneoff` filter, so a `compose run` makes `stop` fail after production is already gated | Skip `oneoff=True` in capture (as Plan 140 does), or fail in `preflight` |
| 4 | Script default `--api-url` is `:5050`; the ops API is `:8060`, and §10.7's commands omit the flag | Change the default to 8060 and fix §10.7 |
| 5 | `/var/lib/containerd` is 29 GB and unmeasured — the storage runbook watches `/var/lib/docker` (714 MB) | Add the containerd store to the storage runbook's disk check; decide a reclaim policy separately |
| 6 | `cartracker-archiver-run-44d23542ea2c` has been running 8 h — a leaked ad-hoc `compose run` | Clean up; consider whether one-offs need a max age |
| 7 | `iproute2` is networking but matches no `PACKAGE_BOUNDARIES` prefix | Add it, or accept and document that the network boundary is netplan-only |
