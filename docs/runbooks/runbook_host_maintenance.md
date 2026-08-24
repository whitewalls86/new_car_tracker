# Runbook: Planned Host Maintenance

Operational companion to [Plan 142](../plans/plan_142_planned_host_maintenance.md).
Covers pausing production, updating Ubuntu, rebooting the single production VM,
proving the host and stack healthy, and resuming work.

**This is Stage 0 output: a worked example, not yet a procedure.** The whole
sequence below is what was actually run on 2026-08-18, recovered verbatim from
the session transcript. Stage 2 turns it into `scripts/host_maintenance.sh`
with idempotent subcommands; until then this is the record you read before you
start, and the thing you copy commands out of.

Everything here is one host: `147.224.199.86`, Compose project `cartracker` in
`/opt/cartracker`. The SSH key lives in the repo root and is gitignored
(`.gitignore:18`) — it is never committed, and this runbook does not reproduce it.

---

## 1. The 2026-08-18 window, end to end

Ubuntu maintenance was combined with the Plan 135 Stage 5 deployment because
both needed the same production pause. It succeeded. Total user-visible
downtime was **about three minutes**; the whole window, including the
investigation that made it safe, ran from 03:59 to roughly 04:26 UTC.

| Time (UTC) | Phase | What happened |
|---|---|---|
| 03:59:00 | preflight | Baseline captured: OS, kernel, `reboot-required`, apt index age, upgradable set, holds, Docker packages, unattended-upgrades state |
| 03:59:21 | preflight | `apt-get update` — **blocked on a lock** |
| 03:59:37 | investigate | Lock traced to `apt-daily.service`, PID 1446391 |
| 04:00:02 | investigate | Its children, open network connections, sources and timers enumerated before touching it |
| 04:03:12 | mutate | `apt-daily.service` stopped; lock confirmed released; `dpkg --audit` clean |
| 04:03:28 | mutate | All four apt units stopped, index refreshed, upgrade **simulated** (`-s`), timers restored and verified |
| 04:06:23 | preflight | Full host check: repo state, disks, `findmnt --verify`, `sshd -t`, `netplan generate`, kernel and boot image, Docker, apt locks |
| 04:06:41 | surprise | A new lock holder, PID 782991 — restoring the timers had let `unattended-upgrades` start **inside the window** |
| 04:06:52–04:07:44 | wait | Three successive poll loops until the installer exited; `dpkg --audit` clean after |
| 04:15:22 | verify | Kernel `6.8.0-1058-oracle` installed, `ops` health OK, 26 containers up, timers active |
| ~04:20 | manifest | `trawl` and `redis-trawl` stopped explicitly; restart policy and Compose labels captured for the auxiliary containers |
| 04:21:54 | stop | `cartracker-mlflow` and `cartracker-lakehouse` projects stopped, running set recorded, `sync`, `reboot` |
| 04:22:27 | reboot | Host back up on `6.8.0-1058-oracle` |
| 04:23:05 | validate host | Mounts, disks, `dockerd --validate`, Docker 29.1.3 |
| 04:23:19 | start | `docker compose up -d --force-recreate` |
| 04:24:53 | validate stack | Compose status, health endpoints, log configs, Promtail errors |
| 04:25:36 | investigate | Docker/Promtail compatibility review |
| after | start | `docker compose up -d --force-recreate trawl redis-trawl` — the solver, by name |

### Before and after

| | Before (04:06) | After (04:23) |
|---|---|---|
| OS | Ubuntu 22.04.5 LTS | unchanged |
| Kernel | `6.8.0-1049-oracle` | `6.8.0-1058-oracle` |
| Docker | 29.1.3 | 29.1.3 |
| `/` | 65% (32 of 49 GB) | **49%** (24 of 49 GB) |
| `/mnt/data` | 34% (64 of 196 GB), 17% inodes | unchanged |
| Git revision | `f62cd00` | unchanged |
| Containers | — | 26 up |

`/` fell 16 points because the Plan 135 Stage 5 log work ran in the same window,
not because of the package transaction.

Two preflight details worth keeping, both required by the plan's own preflight
list and both easy to lose: the `/mnt/data` mount UUID is
`e20a83a2-dd74-47d3-9208-1d2243d68236`, and `findmnt --verify` returns three
warnings on this host in normal operation — including `cannot detect on-disk
filesystem type`. Knowing they are the baseline is what stops someone treating
them as a symptom mid-window.

**A finding nobody has recorded:** `netplan generate` warned that
`/run/netplan/enp0s6.yaml` has permissions "too open" and "should NOT be
accessible by others". It validated successfully, so it did not block the
window, and it appears in no plan or issue. It is unrelated to maintenance and
belongs to whoever owns host hygiene.

**The apt index was 68 days stale** — last updated 2026-06-11, measured
2026-08-18. A `reboot-required` was already pending for two earlier kernels
(`1050`, `1054`) before this window started.

---

## 2. Preflight — everything observable, before anything stops

Run this first. It mutates nothing, so it is safe to run days ahead, and the
output is the baseline you compare against on the way out.

```bash
ssh -i ssh-key-2026-04-08.key ubuntu@147.224.199.86 '
  printf "OS\n"; . /etc/os-release; printf "%s %s\n" "$PRETTY_NAME" "$(uname -r)"
  printf "REBOOT_REQUIRED\n"
  if test -f /var/run/reboot-required; then
    cat /var/run/reboot-required; cat /var/run/reboot-required.pkgs 2>/dev/null
  else echo no; fi
  printf "APT_LIST_AGE\n"; stat -c "%y" /var/lib/apt/lists 2>/dev/null
  printf "UPGRADABLE\n"; apt list --upgradable 2>/dev/null
  printf "HELD\n"; apt-mark showhold
  printf "UNATTENDED\n"; systemctl is-enabled unattended-upgrades; systemctl is-active unattended-upgrades
'
```

> **`HELD` should list `docker.io` and nothing else.** An empty list means the
> hold was lost; anything extra means a package is silently receiving no
> security updates. Both are findings, not noise.

> **Read `APT_LIST_AGE` before you trust `UPGRADABLE`.** On 2026-08-18 the index
> was 68 days old, so the upgradable list at preflight was computed against a
> June catalogue and was not the transaction that actually ran. Refresh first,
> then re-read, or you are planning against fiction.

Then the host-integrity checks, which are the ones that decide whether a reboot
is safe at all:

```bash
ssh -i ssh-key-2026-04-08.key ubuntu@147.224.199.86 '
  set -e
  printf "REPO\n"; cd /opt/cartracker; git status --short --branch; git log -1 --oneline
  printf "DISKS\n"; df -h / /boot /mnt/data; df -i /mnt/data
  printf "MOUNT\n"; findmnt --verify --verbose | tail -5; findmnt /mnt/data
  printf "SSHD\n"; sudo sshd -t && echo valid
  printf "NETPLAN\n"; sudo netplan generate && echo valid
  printf "KERNELS\n"; uname -r; ls -1 /boot/vmlinuz-*oracle | tail -8
  printf "DOCKER\n"; systemctl is-active docker; docker compose version
  printf "APT_LOCKS\n"; sudo fuser /var/lib/dpkg/lock-frontend /var/lib/apt/lists/lock /var/cache/apt/archives/lock 2>/dev/null || true
'
```

`sshd -t` and `netplan generate` matter more than they look. They validate the
two configurations that, if broken, leave you locked out of a rebooted host
with only the Oracle Cloud console as a way back.

**Verify console access before you need it.** The plan requires it and the
August window did not record doing it. If SSH does not come back, the console
is the only route in.

Last, the Airflow maintenance gate — it fails silently in both directions, so
it is a preflight line item rather than something you notice:

```bash
docker exec cartracker-airflow-scheduler airflow pools list
```

> **`maintenance` must be present, with 16 slots.** Missing means the five
> mutating tasks are not being scheduled at all and nothing has said so; 0
> slots means a previous window's hold was never released. See §9.

---

## 3. A stuck apt is the first thing you will hit

`apt-daily` had been running for **66 days**. It held the lock, so nothing could
proceed.

**Do not kill it blindly.** Interrupting an active `dpkg` transaction is far
worse than waiting. Establish what it is doing first:

```bash
ssh ... 'ps -o pid,ppid,etime,stat,args -p <PID>
  systemctl status apt-daily.service apt-daily-upgrade.service --no-pager -l | tail -40
  journalctl -u apt-daily.service -u apt-daily-upgrade.service --since "2 hours ago" --no-pager | tail -60'
```

Only once it is clearly idle rather than mid-transaction, stop it and confirm
both that the lock released and that the package database is consistent:

```bash
ssh ... 'sudo fuser -v /var/lib/apt/lists/lock 2>&1 || true
  sudo systemctl stop apt-daily.service
  sudo fuser -v /var/lib/apt/lists/lock 2>&1 || true
  sudo dpkg --audit'
```

A clean `dpkg --audit` is the gate. It prints nothing when all is well.

---

## 4. The trap: restoring the timers starts an upgrade you did not authorise

This is the sharpest lesson in the window, and the easiest to repeat.

The controlled sequence stopped all four apt units, refreshed the index,
**simulated** the upgrade, and then restored the timers — all in one command:

```bash
sudo systemctl stop apt-daily.timer apt-daily-upgrade.timer \
                    apt-daily.service apt-daily-upgrade.service
sudo apt-get update
sudo apt-get -s upgrade          # -s: simulate. Review before committing.
sudo systemctl start apt-daily.timer apt-daily-upgrade.timer
```

Three minutes later the lock was held again, by PID 782991: restoring the
timers had let `unattended-upgrades` fire **inside the maintenance window**. It
was allowed to finish — the right call, since interrupting it is the worse
option — but package authority had left the operator's hands, and the window
then contained an install nobody reviewed.

It took three separate polling loops to wait it out, because the first two
watched the wrong thing (the PID, then the service's `ActiveState`, then both).

**The fix for next time: do not restore the timers until the window is over.**
Restore them after the reboot and the resume gate, as an explicit final step.
Plan 142 Stage 2 encodes that ordering; it is not a matter of remembering.

`apt-mark showhold` was **empty** on 2026-08-18 — nothing was held. Plan 142
Stage 0 item 4 decided on 2026-08-23 to hold **`docker.io` only**: a daemon
upgrade restarts every container, and `docker.io` is published in
`jammy-security/universe`, which *is* an allowed origin for unattended-upgrades.
Kernel and security updates stay automatic — a kernel installs inert and takes
effect only on a reboot you choose.

Because a held package gets no automatic security fixes, **`apt-mark showhold`
is a preflight line item** (§2) and draining the held class is part of what a
window is for.

---

## 5. Stop, in the right order

Capture the running set *before* stopping anything. The Compose labels are what
make the restore reproducible:

```bash
for name in cartracker-mlflow cartracker-lakekeeper cartracker-lakekeeper-postgres \
            cartracker-trawl cartracker-redis-trawl; do
  docker inspect "$name" --format '{{.Name}} restart={{.HostConfig.RestartPolicy.Name}} \
project={{index .Config.Labels "com.docker.compose.project"}} \
service={{index .Config.Labels "com.docker.compose.service"}}'
done
```

Then stop the profile-gated services and the sibling projects explicitly, record
what is left, flush, and reboot:

```bash
docker stop --time 60 cartracker-trawl cartracker-redis-trawl
cd /opt/cartracker
docker compose -f docker-compose.mlflow.yml   -p cartracker-mlflow   stop
docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse stop
docker ps --format '{{.Names}} {{.Status}}'   # the manifest
sudo sync
sudo reboot
```

`stop`, not `down` — containers, networks and their configuration are preserved.
Use `down` only when a reviewed change requires recreation.

See [`maintenance-running-set.txt`](../../maintenance-running-set.txt) for which
services are expected back and which are deliberately not.

---

## 6. Validate the host before you start anything

Order matters: a stateful service started onto a wrong mount is worse than a
service that stayed down.

```bash
ssh ... 'set -e
  printf "BOOT\n"; uptime -s; uname -r
  printf "MOUNTS\n"; findmnt /; findmnt /mnt/data
  test "$(findmnt -n -o SOURCE /mnt/data)" = /dev/sdb
  printf "DISKS\n"; df -h / /mnt/data; df -i /mnt/data
  printf "DOCKER\n"; sudo dockerd --validate --config-file=/etc/docker/daemon.json
  docker info --format "driver={{.LoggingDriver}} server={{.ServerVersion}}"
  printf "RUNNING_BEFORE_START\n"; docker ps --format "{{.Names}} {{.Status}}"'
```

The `test` on `/dev/sdb` is the important line — it fails the whole command if
`/mnt/data` came back on the wrong device. Expected: `/` on `/dev/sda1`,
`/mnt/data` on `/dev/sdb`, `dockerd --validate` printing `configuration OK`.

---

## 7. Start, and prove it

```bash
cd /opt/cartracker
docker compose up -d --force-recreate
docker compose up -d --force-recreate trawl redis-trawl   # the solver, by name
```

> **The second line is not optional and is easy to forget.** `trawl` and
> `redis-trawl` are profile-gated, so the first command **silently skips them**.
> On 2026-08-18 they came back only because someone remembered to name them. A
> restore that stops after the first line leaves the site up and every detail
> scrape failing — which is the 2026-08-14 solver outage, arrived at a different
> way.

Then verify by evidence, not by container status:

```bash
docker compose ps
curl -sS -o /dev/null -w "ops %{http_code}\n"     http://localhost:8060/health
curl -sS -o /dev/null -w "loki %{http_code}\n"    http://localhost:3100/ready
curl -sS -o /dev/null -w "airflow %{http_code}\n" http://localhost:8080/api/v2/monitor/health
docker logs --since 10m cartracker-promtail 2>&1 | grep -Ei "error|denied|failed" | tail -30
```

Confirm the auxiliary projects are **still stopped**. Plan 140's health metric
cannot tell you this — it filters on the `cartracker` project label and drops
everything else — so it is a separate check against the manifest.

---

## 8. The Promtail break was not caused by a Docker upgrade

Promtail 2.9.8's container discovery failed against the Docker daemon's minimum
API version. The fix shipped as PR #209 (`b94cfd6`, branch
`plan-135-stage-5-docker29`).

**It is tempting to read that as "a host update broke an application." It was
not.** `/var/log/apt/history.log`, read 2026-08-23, settles it:

```
Start-Date: 2026-05-19  20:20:00
Commandline: apt-get install -y docker.io docker-compose-v2 git
Requested-By: ubuntu (1001)
Install: ... docker.io:arm64 (29.1.3-0ubuntu3~22.04.2) ...
```

`docker.io` was **installed once, already at 29.1.3**, by hand during the Plan
105 VM build. It has never been upgraded, and none of the 98
`unattended-upgrade` transactions in this host's history has touched it. The
daemon's minimum API version has been 1.44 since the host existed.

So the incompatibility was **latent from day one**. What changed in the window
was Plan 135 Stage 5 pointing Promtail at Docker service discovery for the first
time. An application change revealed a pre-existing boundary.

The lesson worth carrying is different from the one this looked like: **a
maintenance window surfaces latent incompatibilities precisely because it is the
first time in months that everything is recreated at once.** That argues for
thorough post-restore verification — §7's checks — rather than for pre-install
package review, which could not have caught this.

---

## 9. The maintenance pool — the gate, and how it is held

**Plan 142 Stage 0 item 3, Phase A. Deployed and soaking; the hold itself
(Phase B) has not been exercised.** This section is the operator-facing half of
[`airflow/dags/pools.py`](../../airflow/dags/pools.py), which carries the
reasoning.

Five tasks — every one that can mutate production — sit in an Airflow pool
called `maintenance`:

| DAG | Task | Why it is the gate point |
|---|---|---|
| `results_processing` | `process_batch` | The only writer of `last_detail_scraped_at` |
| `orphan_checker` | `expire_orphan_detail_claims` | Janitorial SQL against `ops` |
| `orphan_checker` | `reap_stuck_processing` | ditto |
| `orphan_checker` | `evict_delisted_cooldowns` | ditto |
| `scrape_detail_pages` | **`claim_batch`**, not `scrape_detail` | Hold the claim and nothing is ever claimed. Hold the scrape and a batch is claimed, then stranded for the window |

`scrape_listings` is deliberately **not** held. It advances a rotation over
`search_configs` and never consults processed state, so it cannot loop; it only
adds SRP artifacts to the durable pending backlog, which is the condition the
drain contract wants to observe.

`scrape_detail_pages` is held even though only `results_processing` looks like
the mutating one, and the reason is a circuit breaker. `ops.ops_detail_scrape_queue`
(a view, `V040__detail_scrape_circuit_breaker`) selects listings whose
`price_observations.last_detail_scraped_at` is null or older than 7 days. That
column is written in exactly one place — `processing/writers/detail_writer.py:194`,
the processing service — while `POST /scrape/claims/release` **deletes** the
claim. So holding processing alone means the scraper claims ~100 listings,
scrapes them, releases, nothing marks them scraped, and fifteen minutes later
it claims **the same listings again**: up to four redundant passes an hour,
real fetches against cars.com through the solver Plan 136 is nursing. Holding
processing without holding the detail scraper *disables the circuit breaker and
leaves its producer running.* Plan 147 fixes the ownership properly; until it
lands, the two are held together.

### Holding and releasing

```bash
# Hold. Durable: survives scheduler restart, container recreate, host reboot.
docker exec cartracker-airflow-scheduler \
  airflow pools set maintenance 0 "Plan 142 window <date>"

# Release. Nothing else releases it -- that is the point.
docker exec cartracker-airflow-scheduler \
  airflow pools set maintenance 16 "released <date>"
```

> **Why `pools set 0` and not a hold task.** Plan 142's Stage 0 item 3 table
> praises the pool because "a hold task that dies releases its slots and normal
> scheduling resumes on its own." That is right for Plan 136 and **backwards
> for Plan 142**, whose first design principle is that maintenance must never
> auto-release — not on a lease expiry, a dropped shell, an `EXIT` trap or a
> reboot. `pools set 0` is a row in the Airflow metadata DB and stays put
> through all of them. The cost is that it also stays put through *forgetting*,
> which is the direction this plan chooses to fail in, and which is why a
> "pool held for longer than N minutes" alert is owed by Stage 1.

> **The pool is not created from git, deliberately.** `airflow pools set` is an
> upsert, so a declarative create in `airflow-init` would reset the slot count
> on every `docker compose up -d` — and the slot count *is* the hold state. A
> maintenance window recreates the stack, so that would silently release the
> hold mid-window. `tests/airflow/test_maintenance_pool.py` asserts Compose
> never sets a pool.

### Two things to check before trusting a quiet fleet

Both are preflight items, and both fail *silently* — no task failure, no alert:

```bash
docker exec cartracker-airflow-scheduler airflow pools list
```

Verbatim, as of 2026-08-24 — anything else is a finding:

```
pool         | slots | description               | include_deferred
=============+=======+===========================+=================
default_pool | 128   | Default pool              | False
maintenance  | 16    | Plan 142 maintenance gate | False
```

1. **The pool must exist.** It lives only in the Airflow metadata DB, so a
   rebuilt DB loses it. When it is missing the scheduler logs
   `Tasks using non-existent pool 'maintenance' will not be scheduled`
   (`scheduler_job_runner.py:693`) and all five tasks above simply stop
   running. This is also the deploy ordering rule: **create the pool before the
   DAG code lands, never after.**
2. **The slot count must be 16 outside a window.** A count of 0 left behind is
   an un-released hold wearing the same face as a healthy fleet.

### What the hold does and does not do to a task

Verified against `apache-airflow-core` 3.2.0, because the acceptance contract
turns on it. A pool-starved task instance stays in `SCHEDULED`: the scheduler
sees `open_slots <= 0`, logs "Not scheduling since there are 0 open slots", and
skips it (`scheduler_job_runner.py:703`). It never reaches `QUEUED`, so it never
gets a `queued_dttm`, so `_get_tis_stuck_in_queued` — the only thing that fails
a task for waiting, at `[scheduler] task_queued_timeout` = 600s — cannot see it
(`scheduler_job_runner.py:2472`).

**That is the whole argument for this mechanism.** An hour-long pause creates no
failed DAG. The deploy-intent sensor cannot say the same: its own 600s timeout
failed two `check_deploy_intent` tasks in the window recorded in §1.

The sensors are therefore **not** pooled. A `reschedule`-mode sensor must not
hold a slot while it waits, and both factories in `airflow/dags/sensors.py` take
`**kwargs`, so `pool=` would sail straight through — the test asserts no DAG
ever passes one.

### What the pool cannot do

A pool gates; it does not count. It cannot distinguish durable pending backlog
from a running claim — the distinction the drain contract turns on — and it is
blind to work this scheduler did not start: `ops` HTTP endpoints and
long-running pack/prune sections among them. It replaces one mechanism inside
the drain contract, never the contract.

---

## 10. Gaps in this record

Everything Plan 142 Stage 0 item 1 asks for is present: timeline, commands,
failure modes, intended-stopped services, and recovery evidence — all from the
primary record rather than reconstructed.

**Settled, and recorded above rather than outstanding:**

- **Console access was not verified before the reboot.** Every mention of the
  Oracle Cloud console in that session is Plan 142's own text being *drafted*,
  at the end of the same window. The plan's preflight requires the check
  precisely because this window skipped it.
- **The pre-window disk baseline exists** — `/` at 65%, captured at 04:06:23.

**Genuinely still on the host.** Neither is required by item 1; the first is
enrichment, and the second is a question this recovery raised rather than one it
was asked to answer:

Both were read on 2026-08-23 and are now closed. The window's automatic
transaction was eight `unattended-upgrade` runs between 04:05:27 and 04:06:37
(`tzdata`, `ncurses-base`, `libpam-runtime`, `systemd` and five siblings,
`dnsmasq-base`, `perl`, `python3.10`, `distro-info-data`) — and `docker.io` was
not among them, or among anything else, ever (§8).

Nothing outstanding. The record is complete.

---

## 11. Related

- [Plan 142](../plans/plan_142_planned_host_maintenance.md) — the state machine,
  drain contract and stage plan this runbook serves.
- [`maintenance-running-set.txt`](../../maintenance-running-set.txt) — what is
  expected running, what is deliberately stopped, and why.
- [Storage maintenance](runbook_storage_maintenance.md) — the monthly disk
  check. Its §3 lists the three commands that can destroy this host.
- [Solver OOM and recycle](runbook_solver_oom_and_recycle.md) — `trawl`
  restarts that do *not* need a maintenance window.
