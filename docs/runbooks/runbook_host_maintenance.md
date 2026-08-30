# Runbook: Planned Host Maintenance

Operational companion to [Plan 142](../plans/plan_142_planned_host_maintenance.md).
Covers pausing production, updating Ubuntu, rebooting the single production VM,
proving the host and stack healthy, and resuming work.

**Most of this remains Stage 0 output: a worked example, not a record of a new
window.** The whole sequence below is what was actually run on 2026-08-18,
recovered verbatim from the session transcript. The checked-in
[`scripts/host_maintenance.py`](../../scripts/host_maintenance.py) now carries
the reviewed coordination, package, reboot, validation, completion, and apt
restoration commands; §10.7 is the authoritative close-out sequence. The
earlier sections remain the record to read before starting and the source from
which reviewed commands are taken.

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
two configurations most likely to leave you locked out of a rebooted host.

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
| `results_processing` | `process_batch` | The only writer of the enrichment timestamp |
| `orphan_checker` | `expire_orphan_detail_claims` | Janitorial SQL against `ops` |
| `orphan_checker` | `reap_stuck_processing` | ditto |
| `orphan_checker` | `evict_delisted_cooldowns` | ditto |

**`scrape_detail_pages` was held here until 2026-08-30 and no longer is.** It
was in the pool for one reason: pausing `results_processing` used to leave the
detail scraper re-claiming the same listings every 15 minutes, because the
guard against re-fetching was written by the processing service. [Plan
147](../plans/plan_147_scrape_state_ownership.md) moved that guard into
`release_claims`, and a production run on 2026-08-30 paused processing for 81
minutes and saw 2,000 fetches with zero repeats. **Pausing processing no longer
requires pausing the scraper.**

If you are quiescing detail fetches for a host reboot, that is a different
mechanism and it still works — the `detail_fetch` coordination surface, not
this pool. Do not restore the pool assignment to achieve it.

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

And once a `*/5` run has landed, the proof that the assignment reached real
task instances rather than merely existing in the DAG source:

```bash
docker exec cartracker-postgres psql -U cartracker -d cartracker -At -F'|' -c \
  "select pool, dag_id, task_id, state, count(*)
     from airflow.task_instance
    where start_date > now() - interval '20 minutes'
    group by 1,2,3,4 order by 1 desc, 2, 3;"
```

> **Two details this query gets wrong if copied from habit.** The Airflow
> metadata lives in the **`airflow` schema** of the `cartracker` database, not
> in `public`, so bare `task_instance` raises `relation does not exist`. And
> the container has **no `postgres` role** — `docker exec -u postgres` fails
> with `role "postgres" does not exist`. Use `-U cartracker`.

Five task IDs should appear against `maintenance`; everything else stays on
`default_pool`. This is the only check here that can tell a working deployment
from one that never landed — `airflow dags list-import-errors` returning
`No data found` and the pool sitting at 16 slots look identical either way.

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

> **The deploy-intent sensor has a second defect, and it is live.** During a
> deploy it never records the gate observation the coordination drain waits
> on, so a deploy declared while a gated DAG run is live hangs indefinitely —
> §11's *When the drain never drains* carries the recognition and the escape.
> It is not the pool's problem, but it wears the same face: a fleet that has
> gone quiet and stays that way.

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

## 10. The Stage 0 window (Phase B), end to end

**Not yet run.** This is the approved shape of the window that closes Plan 142
Stage 0: it deploys items 6 and 7, then holds the maintenance pool for an hour
and measures what the queued backlog does on release. About **90 minutes, of
which ~3 are user-visible.**

**No host operation is involved** — no packages, no reboot. Sections 3 through 6
do not apply. Section 2's preflight does, minus its apt and kernel lines.

> **This section predates Plan 142 Stage 1's coordination record, which is live
> in production as of 2026-08-25.** The two do not conflict, but know which is
> doing what:
>
> - **10.2 and 10.3 mutate containers.** Drive them through
>   `scripts/redeploy.sh` where you can, which requests coordination, drains,
>   authorizes and releases around the change. The raw `docker compose` lines
>   above are the fallback when a service has no build context or you are
>   recovering.
> - **10.4's hold is the `maintenance` Airflow pool, not coordination.** Setting
>   the pool to 0 slots does not touch `coordination_state`, and
>   `/coordination/status` will read `phase=none` throughout the hold. Do not
>   read that as the gate being off.
> - **Never leave a coordination window open across the hold.** The admission
>   sensor blocks on `phase IN (requested, draining, active, validating)`, so an
>   un-released deploy record would stop the very DAGs whose queuing behaviour
>   10.4 is trying to measure, and you would misattribute the result.

| Step | What | User-visible | Data risk | Abort |
|---|---|---|---|---|
| 10.1 | Preflight | none | none | stop; nothing has changed |
| 10.2 | caddy restart policy (item 7) | ~10s on cartracker.info | none | revert commit, `up -d caddy` |
| 10.3 | HMAC rotation (item 6) | Airflow UI ~1 min | none — tokens re-issued | restore `.env`, `up -d` the four |
| 10.4 | The hold, ~60 min | none | none — backlog is durable | `pools set maintenance 16` |
| 10.5 | Release, and measure | none | load spike on `orphan_checker` | `pools set maintenance 0` |
| 10.6 | Restore | none | none | — |

Every step reverts without touching data. **The order is deliberate:** caddy
first because it is the shorter blip and its abort is cleanest, then the
rotation — which wants a quiet moment, since it invalidates in-flight worker
tokens — and only then the hold. Rotating *during* a hold would confuse two
causes of an idle fleet.

### 10.1 Preflight

Read-only, and safe to run days ahead. Section 2's first two blocks, minus apt
and kernel, plus the four things this window specifically needs as a baseline
to compare against on the way out:

```bash
docker exec cartracker-airflow-scheduler airflow pools list
docker inspect --format '{{.HostConfig.RestartPolicy.Name}}' caddy   # expect: no
docker exec cartracker-postgres psql -U cartracker -d cartracker -At -F'|' -c \
  "select state, count(*) from airflow.task_instance
    where start_date > now() - interval '1 hour' group by 1;"
docker exec cartracker-postgres psql -U cartracker -d cartracker -At -c \
  "select intent from deploy_intent;"
```

Stage 2's checkpoint writer now exists. Every successful transition appends one
JSON line to `/var/lib/cartracker/maintenance/history.jsonl`, with only phase,
UTC timestamp, Git revision, running kernel, and manifest location. The client
is replay-safe: if Postgres advanced but the local append failed, rerunning the
same command repairs the breadcrumb without repeating the transition.
The file is explicitly an offline operator convenience derived only after the
Postgres-backed API confirms a phase. It is never reconciled back into Postgres
and is not the durable transition history: V044's
`staging.coordination_state_events` records each mutation in the same database
transaction and the existing staging-event processor archives those rows.

The running-set manifest is also executable restore authority, not just an
inventory. The client derives an exact per-project plan from containers recorded
as running, including required Compose profiles. It refuses a manifest that
marks a one-shot, on-demand, deliberately paused, or foreign service as running;
it never discovers restore targets by walking the filesystem.

Run the checked-in preflight. It is observation-only: it does not refresh apt,
stop a service, install a package, or change coordination state.

```bash
EVIDENCE=/var/lib/cartracker/maintenance/$(date -u +%Y%m%dT%H%M%SZ)
python scripts/host_maintenance.py preflight --output-dir "$EVIDENCE"
MANIFEST="$EVIDENCE/running-set.json"
```

The evidence directory contains `preflight.json`, `running-set.json`, and one
sanitized structural Compose record per project under `compose/`. Full Compose
renders are validated in memory but never written because interpolation may
contain credentials. Preflight refuses an empty Docker host, an active apt/dpkg
lock, non-empty `dpkg --audit`, or any held-package set other than the reviewed
`docker.io` hold. A refusal is a finding to investigate, not a reason to bypass
the check.

The currently available sequence is:

```bash
python scripts/host_maintenance.py plan
python scripts/host_maintenance.py prepare-update \
  --output-dir "$(dirname "$MANIFEST")" --include-held docker.io
python scripts/host_maintenance.py --manifest "$MANIFEST" request \
  --requested-by "$USER" --reason "reviewed host maintenance" \
  --expected-work "install reviewed packages" --expected-work reboot
python scripts/host_maintenance.py --manifest "$MANIFEST" drain
python scripts/host_maintenance.py --manifest "$MANIFEST" wait-active
python scripts/host_maintenance.py --manifest "$MANIFEST" stop
python scripts/host_maintenance.py --manifest "$MANIFEST" update \
  --package-plan "$PACKAGE_PLAN" --confirm-plan "$PACKAGE_PLAN_SHA256" \
  --confirm-apply --release-notes-reviewed --compatibility-reviewed
python scripts/host_maintenance.py --manifest "$MANIFEST" reboot --confirm-reboot
# reconnect after the VM returns, then prove the boot changed:
python scripts/host_maintenance.py --manifest "$MANIFEST" reboot
python scripts/host_maintenance.py --manifest "$MANIFEST" start
python scripts/host_maintenance.py --manifest "$MANIFEST" begin-validation
```

`plan` is a non-mutating dry run of Stage 2's canonical ordering. It ends at
`begin-validation`: the Stage 3 evidence gates own `validate-host`,
`validate-stack`, apt-automation restoration, and explicit `complete`. There is
no exit trap or Stage 2 failure path that can release coordination.

Record the printed package-plan SHA-256 and review every pinned version plus
the named compatibility boundaries before entering the drain. Preparation
refreshes indexes and downloads the exact combined transaction but installs
nothing. `--include-held` must name the complete current hold set, so a held
runtime package cannot silently age outside the reviewed transaction.

`update` will not accept a changed plan: `--confirm-plan` must be the exact
SHA-256 printed by preparation. The three confirmation flags attest that the
operator intends installation and has reviewed release notes and every named
compatibility boundary. The command records and masks apt automation, applies
only pinned versions, rechecks locks before masking, audits dpkg, verifies the
installed versions and hold set, syncs, and leaves automation masked for
restoration only after the Stage 3 resume gate.

The first `reboot` invocation syncs and writes `rebooting` before asking systemd
to reboot; it does not claim success when that command returns. After reconnect,
rerun `reboot` without the confirmation flag. The client compares the live Linux
boot ID to the preflight manifest and writes `rebooted` only when they differ.

`wait-active` polls drain evidence every five seconds but prints structured
progress no more than once per minute. It has no short overall deadline; stale
coordination alerts separately. An individual API request still has a ten-second
timeout, which is logged with its method and route before the command fails
closed. Use `drain-status` when an operator needs an immediate evidence dump.

After `active`, `stop` and `start` deliberately use the last durable local
checkpoint because Postgres and the coordination API may be offline. They run
only the Compose commands derived from the preflight manifest, verify every
selected container reached the requested state, and checkpoint the result.
Rerunning either command repeats the same idempotent Compose operation; it never
walks the host for additional projects or services.

`complete` is intentionally unavailable until the Stage 3 resume-gate sequence
below has recorded both evidence halves. A successful command is an explicit
release of coordination authority, not an implicit consequence of recovery.

### 10.2 caddy's restart policy — Plan 142 Stage 0 item 7

Ships in git ([docker-compose.yml](../../docker-compose.yml)), so the VM pulls
it. **~10 seconds of user-visible downtime on cartracker.info.**

```bash
cd /opt/cartracker
docker compose up -d --no-deps caddy
docker inspect --format '{{.HostConfig.RestartPolicy.Name}}' caddy
curl -sS -o /dev/null -w "%{http_code}\n" https://cartracker.info
```

> **Corrected 2026-08-25, both lines, after running this step.**
>
> **There is no `git pull` here.** `restart: unless-stopped` reached the VM's
> checkout in **PR #232**, well before the deploy this window follows. Pulling
> as part of 10.2 drags in whatever else has merged since and makes a
> ten-second caddy recreate into an unbounded change. Confirm the key is
> already in the file instead:
> `grep -A2 'container_name: caddy' docker-compose.yml`.
>
> **`--no-deps` is load-bearing.** Run bare, `docker compose up -d caddy` walks
> the dependency graph: on 2026-08-25 it printed `Container
> cartracker-postgres Waiting` / `Healthy` and re-ran `cartracker-airflow-init-1`
> — work nobody asked for, during a window whose whole point is a bounded
> blast radius. This is `redeploy.sh` decision 1 restated; that script passes
> `--no-deps` for exactly this reason.

Expect `302` from the curl, not `200` — `/` redirects to the auth proxy. Any
response at all proves caddy is serving.

> **`up -d`, never `restart`.** This is a service *config* change.
> `docker compose restart` reuses the existing container and its old config, so
> the policy would be silently unapplied while everything looked healthy — the
> exact trap Plan 135 hit on `node-exporter`, where the container came back
> looking fine with the old flags still in place.

> **Verify the policy, not the uptime.** `docker inspect` must print
> `unless-stopped`. Plan 135's rule was "always check `.Args`, not container
> uptime"; this is the same rule against a different field. A container that is
> up proves nothing about what happens after a reboot.

TLS material lives in the `caddy_data` volume and is untouched by the recreate.
Nothing resolves `caddy` by name — it is the ingress, not an upstream — so
[deploy-followers.txt](../../deploy-followers.txt) has no entry to honour here.

**Abort:** revert the commit, `docker compose up -d caddy` again.

### 10.3 Rotate `AIRFLOW_JWT_SECRET` — Plan 142 Stage 0 item 6

**Not in git.** The value lives only in the VM's `/opt/cartracker/.env`, which is
why this step is a runbook procedure rather than a commit.

The current key is 35 bytes, under the 64 RFC 7518 §3.2 recommends for
HMAC-SHA512, so PyJWT emits `InsecureKeyLengthWarning` on every apiserver start.
**This is hygiene, not a vulnerability** — the key is a generated string at
~4.5 bits of entropy per character, far past brute force, and the RFC's rule
compares key length to hash output rather than describing a break. It rides a
window because rotation invalidates in-flight worker tokens, not because it is
urgent.

```bash
cd /opt/cartracker
cp .env .env.bak-jwt-$(date -u +%Y%m%dT%H%M%SZ)
python3 -c 'import secrets; print(secrets.token_urlsafe(64))'
# edit /opt/cartracker/.env -> AIRFLOW_JWT_SECRET=<new value>
docker compose up -d --no-deps airflow-apiserver airflow-scheduler \
                               airflow-dag-processor airflow-triggerer
```

> **`--no-deps` here too**, for the reason given in 10.2 — these four declare
> `depends_on` and a bare `up -d` re-runs `airflow-init`.
>
> **Back up `.env` first.** This value exists nowhere else: it is not in git,
> and losing it mid-rotation leaves you unable to restore the previous state
> that this section's own abort instruction depends on.
>
> **Run 2026-08-25.** Old key 35 bytes, new key 86. All three verifications
> below passed: `InsecureKeyLength` count 0, scheduler heartbeat publishing,
> no DAG import errors. If you keep a copy in a local `.env`, confirm that file
> is gitignored and delete any `.env.bak*` you create outside the VM — a
> backup written next to a repo is a secret in a shared worktree.

`up -d` again, not `restart`: the value arrives through the environment, and a
restarted container keeps the environment it was created with.

Verify all three, in this order:

```bash
# 1. The warning is gone -- the point of the change
docker logs --since 5m cartracker-airflow-apiserver 2>&1 | grep -i InsecureKeyLength

# 2. The scheduler is talking to the apiserver again (Plan 136 D6: back within 80s)
#    statsd-exporter publishes no host port, so this goes through the container.
docker exec cartracker-statsd-exporter \
  wget -qO- http://localhost:9102/metrics | grep airflow_scheduler_heartbeat

# 3. A DAG run completes end to end
docker exec cartracker-airflow-scheduler airflow dags list-import-errors
```

The first must be **empty**. Recreating these four does not orphan anyone:
[deploy-followers.txt](../../deploy-followers.txt) covers recreating
`statsd-exporter`, and these are the *senders* — a restarted sender re-resolves.

**Abort:** restore the previous value in `.env` and `up -d` the four again. Keep
the old value to hand until verification passes; a rotation you cannot undo is
a rotation you should not start.

### 10.4 The hold — the measurement, ~60 minutes

```bash
docker exec cartracker-airflow-scheduler \
  airflow pools set maintenance 0 "Plan 142 Stage 0 Phase B <date>"
```

Then every ~10 minutes record: queued versus running task instances, whether any
task has **failed**, the scheduler heartbeat, and that non-pooled DAGs still run
normally.

```bash
docker exec cartracker-postgres psql -U cartracker -d cartracker -At -F'|' -c \
  "select pool, state, count(*) from airflow.task_instance
    where start_date > now() - interval '2 hours' or state in ('scheduled','queued','running')
    group by 1,2 order by 1,2;"
```

Acceptance, straight from the plan's drain contract:

- **No new mutating task starts.** The five pooled tasks queue.
- **No task fails merely because time passed.** This is the whole point — §9
  explains why it holds by construction, and today's sensor mechanism fails at
  600s and did so twice in August.
- **Unrelated DAGs are unaffected**, which is what proves the gate is scoped
  rather than global. `scrape_listings` is deliberately still running.

**Abort at any point:** `airflow pools set maintenance 16`.

### 10.5 Release, and measure the backlog — the real unknown

```bash
docker exec cartracker-airflow-scheduler \
  airflow pools set maintenance 16 "released <date>"
```

Record how the queued runs drain — roughly **28** will have accumulated per hour
held (12 `results_processing` + 12 `orphan_checker` + 4 `scrape_detail_pages`):
elapsed time to zero, peak concurrency, whether anything errored, and
processing-queue depth before and after.

Then confirm the re-scrape storm did **not** happen — the reason
`scrape_detail_pages` is in the held set at all (§9):

- detail artifacts produced *during* the hold should be ~0;
- `last_detail_scraped_at` for the held listings should advance **exactly once**
  after release, not four times.

> **The risk worth naming in advance.** `orphan_checker` has **no
> `max_active_runs`**, so it defaults to 16 — up to 12 queued runs could fire at
> once against the three `ops` maintenance endpoints. `results_processing` is
> safe at `max_active_runs=1`. The endpoints are idempotent janitorial SQL, so
> the expected blast radius is **load, not corruption**.
>
> **Measure it rather than pre-empting it.** That thundering herd on release is
> exactly the acceptance criterion "resuming does not unleash an unbounded
> duplicate backlog". If it misbehaves, that is the finding, and
> `max_active_runs=1` is the fix Stage 1 ships. Setting it beforehand would
> measure a system already fixed.

### 10.6 Restore

```bash
docker exec cartracker-airflow-scheduler airflow pools list   # 16 slots, used 0
docker inspect --format '{{.HostConfig.RestartPolicy.Name}}' caddy   # unless-stopped
```

Confirm 28 pooled runs an hour are green again, and write the results into
Plan 142's item 3 section — the measurements are the deliverable, and a window
whose findings live only in a terminal has not closed.

### 10.7 Stage 3 resume gate — host maintenance only

Run this only after `start` and `begin-validation` from the host-maintenance
sequence above. It is the closing procedure for a host window, not a deploy or
the Stage 0 pool hold. Keep the same immutable evidence directory and manifest
used for the window:

```bash
EVIDENCE=/var/lib/cartracker/maintenance/<window>
MANIFEST="$EVIDENCE/running-set.json"
PREFLIGHT="$EVIDENCE/preflight.json"
PACKAGE_PLAN="$EVIDENCE/package-plan.json"
PACKAGE_PLAN_SHA256=<the reviewed digest printed by prepare-update>
API=http://localhost:8060

# 1. Collect host facts, evaluate all seven host gates, and submit the passing
#    bundle for the live coordination generation.
python scripts/host_maintenance.py --api-url "$API" --manifest "$MANIFEST" \
  validate-host --preflight "$PREFLIGHT" --output-dir "$EVIDENCE"

# 2. Read the independently re-evaluated stack gate. It must report every gate
#    as pass, including Plan 140 coverage and intentionally stopped auxiliaries.
curl -sf "$API/coordination/release-status"

# 3. Explicitly release only after both evidence halves pass.
python scripts/host_maintenance.py --api-url "$API" --manifest "$MANIFEST" \
  complete --confirm-complete

# 4. Only after completion, restore the exact recorded apt-automation state and
#    prove the reviewed hold set has not drifted.
python scripts/host_maintenance.py --api-url "$API" --manifest "$MANIFEST" \
  restore-apt-automation --package-plan "$PACKAGE_PLAN" \
  --confirm-plan "$PACKAGE_PLAN_SHA256"
```

> **`--api-url` is not optional here.** The client defaults to
> `http://localhost:5050`, which answers **500** on this host; the coordination
> API is on **8060**, as `redeploy.sh` already has it. These commands were
> written without the flag and would have failed as printed — found 2026-08-29
> while scoping the Stage 4 window.

1. **Host evidence — no user-visible change, no data risk.** `validate-host`
   writes `validate-host.json` and submits it only when every host gate passes.
   Abort on any `fail` or `unknown`; investigate the named reason and leave
   coordination in `validating`. Do not edit evidence or retry around a refusal.
2. **Stack evidence — no user-visible change, no data risk.**
   `release-status` is read-only. Confirm all returned gates pass; missing Plan
   140 health data, an unhealthy/unconfigured service, stale observability, or
   a restarted auxiliary is a failed gate. Abort by keeping the window paused;
   repair the cause and rerun the read.
3. **Completion — releases the paused surface.** `complete` re-evaluates stack
   gates server-side and requires the durable host-evidence row for the current
   generation, so its blast radius is resuming normal work. A `409` leaves the
   phase at `validating`; do not force it or use a compatibility release route.
   Fix the named gate, obtain fresh evidence, and repeat this sequence.
4. **Apt restoration — enables background apt work after release.** Its blast
   radius is limited to restoring the previously recorded enablement state;
   package versions do not change. Abort on an enablement or hold-set refusal:
   leave the units as they are, retain the evidence, and investigate. Never
   restore these units before successful `complete`.

Record the `validate-host.json` result, `release-status` response, completion
response, and restored unit states with the window evidence. Completed-window
reconstruction spans the current Postgres coordination records and the archived
Parquet staging-event history; staging rows may be flushed after export.

---

## 11. Stage 1 scoped-coordination verification

The targeted deploy is driven through the compatibility facade because it must
dual-signal long-running legacy consumers during migration. `redeploy.sh` now
owns the complete sequence: it requests the exact named services, expands their
followers and surfaces in the API, begins draining, waits for a confirming
authorization read, mutates, health-checks, enters validation, and releases.

Choose a target whose expansion does not contain `detail_fetch`; `processing`
is the initial fixture, so `trawl` remains unaffected:

```bash
bash scripts/redeploy.sh processing
curl -sf http://localhost:8060/coordination/status | python3 -m json.tool
```

While the script reports that it is draining, inspect the evidence separately:

```bash
curl -sf http://localhost:8060/coordination/drain-status | python3 -m json.tool
```

Interpret every source independently:

- `known` with `count: 0` is drained;
- `known` with a positive count names real admitted work and its oldest start;
- `unknown` is a blocker, never zero;
- `not_applicable` means the selected scope does not require that source;
- `drained: true` is authority only while the returned phase is `draining`.

Before any container changes, interrupting or a failed build releases the
facade coordination. After a recreate/restart begins, failure deliberately
leaves it held. Inspect and restore the target, then explicitly enter validation
and use the compatibility release:

```bash
curl -sf -X POST http://localhost:8060/coordination/begin-validation
curl -sf -X POST http://localhost:8060/deploy/complete
```

The second acceptance run—the non-outage whole-production dry run—waits for
Stage 3. There is intentionally no native `/coordination/complete` or temporary
force-complete endpoint: a whole-scope request that reaches `validating` must
remain held until the host, stack, and intentionally-stopped-service evidence
guard exists. Before Stage 3, its safe rehearsal boundary is `draining`, then
`POST /coordination/cancel`; do not authorize it.

### When the drain never drains — the gate deadlock

**Live in production until [Plan 158](../plans/plan_158_coordination_gate_deadlock.md)
Stage 1 deploys.** A deploy declared while a gated DAG run is live hangs
forever. `_DeployIntentSensor.poke()`
([`airflow/dags/sensors.py:66`](../../airflow/dags/sensors.py)) returns early on
`intent != "none"`, so the `INSERT INTO coordination_gate_observations` beneath
it is unreachable during a deploy — and `airflow_gate_observations` is exactly
the source the drain waits on. **The drain is waiting for a write that cannot
happen while it is waiting.**

**Recognise it.** `redeploy.sh` prints `In-scope work is still draining;
retrying in 5s` and keeps printing it. Read the evidence separately, twice,
about five minutes apart:

```bash
curl -sf http://localhost:8060/coordination/drain-status | python3 -m json.tool
```

```json
{
    "phase": "draining",
    "drained": false,
    "blockers": ["airflow_gate_observations"],
    "sources": [
        {
            "source": "airflow_gate_observations",
            "status": "known",
            "count": 6,
            "oldest_started_at": "2026-08-30T06:00:00.690000+00:00"
        }
    ]
}
```

The signature is the **direction of `count` between the two reads, not its
value**. A healthy drain falls to zero as admitted work finishes — that is what
the second read is for. This one does not fall, and given time it rises:
`orphan_checker` fires every five minutes, each new run parks on the gate and
adds one, and nothing in the loop can ever subtract. `oldest_started_at` stays
pinned to the same instant across both reads, because the run it names will
never end.

> **This state lies when read against the interpretation list above.** *"`known`
> with a positive count names real admitted work and its oldest start"* is true
> of every other source and false of this one. These runs are parked on the
> sensor, holding no work and doing none; the count names their existence, not
> their activity. It is the one case where a positive count is not a reason to
> keep waiting.

Confirmation, if the two reads are not enough — the observations table is empty
for the current generation, and has been for every generation that ever
existed:

```bash
curl -sf http://localhost:8060/coordination/status | python3 -m json.tool   # → "generation": 17
docker exec cartracker-postgres psql -U cartracker -d cartracker -At -c \
  "select count(*) from public.coordination_gate_observations where generation = 17;"
```

```
0
```

§9's role caveat applies here too: the container has no `postgres` role, so use
`-U cartracker`.

**Recover it.** Interrupt `redeploy.sh` — plain `Ctrl-C`. Nothing has been
recreated, and that is guaranteed rather than hoped for: mutation happens only
after `_prepare_coordination` returns, so a script still printing "still
draining" has never reached it. `MUTATED` is therefore `0`, and `_on_exit`
([`scripts/redeploy.sh:161`](../../scripts/redeploy.sh)) takes the release
branch:

```
Signalling deploy complete...
true
```

Intent is released and the fleet resumes by itself; the parked sensors poke
every 60 seconds, so the gate opens within a minute with no further action.

If the trap does not fire — `SIGKILL`, a dropped SSH session, a terminal that
died with the script — post the release by hand:

```bash
curl -X POST http://localhost:8060/deploy/complete
```

```
true
```

Then prove both halves released, because a released intent alone does not say
the coordination row moved:

```bash
curl -sf http://localhost:8060/deploy/status | python3 -m json.tool
curl -sf http://localhost:8060/coordination/status | python3 -m json.tool
```

`intent` reads `none`, `phase` reads `none`, and `generation` is **one higher
than the deploy's** — the release increments it in the same transaction, so an
unchanged generation means the release did not land and the gate is still shut.

**Avoid it.** Pause the affected DAGs and let their in-flight runs finish
before declaring intent. `drain-status`'s `scope` names the surfaces;
`airflow.dag_run` names the runs that would park:

```bash
docker exec cartracker-postgres psql -U cartracker -d cartracker -At -F'|' -c \
  "select dag_id, run_id, state, start_date
     from airflow.dag_run
    where state in ('queued', 'running') order by start_date;"

docker exec cartracker-airflow-scheduler airflow dags pause orphan_checker
# one per affected DAG — and unpause every one of them after the deploy
```

> **Pause; do not try to time it.** A deploy declared into a gap in the schedule
> authorizes immediately, which is the only reason this has ever worked — see
> generation 16, which went active at 02:52 UTC. But `orphan_checker` alone
> fires twelve times an hour, the gap is minutes wide, its boundary is not
> visible from outside, and the deploy is longer than the gap. Timing it is a
> gamble whose losing branch is unbounded.

> **Pausing is not the `maintenance` pool** (§9). A paused DAG creates no run at
> all, so there is nothing for the gate to count; the pool holds a created run
> in `SCHEDULED`, which is precisely the state that blocks here. Both are
> durable in the Airflow metadata DB, and both fail in the same direction: a
> forgotten pause outlives the window wearing the face of a quiet fleet.
> Unpausing is part of the deploy, not something to do after it.

**Why it hangs rather than fails.** `_prepare_coordination`
([`scripts/redeploy.sh:135`](../../scripts/redeploy.sh)) polls
`/coordination/authorize` in a `while :` loop with no timeout and no escape: a
409 sleeps `DRAIN_POLL_INTERVAL` (5s) and retries, forever. It is the one wait
in the script that does not follow the script's own decision 2 — the health
gate fails loudly at 300s and this does not fail at all. Plan 158 Stage 2 bounds
it; until that ships, **the operator is the timeout**, which is why recognising
the shape above matters more than it should.

**The reference incident — 2026-08-30**, deploying `ops` and `processing` for
Plan 147 Stage 2:

| Time (UTC) | Event |
|---|---|
| 05:56:34 | `redeploy.sh ops processing` declares intent; generation 17, phase `draining` |
| 06:00:00 | Seven scheduled DAGs fire and park on the gate sensor |
| 06:04 → 06:15 | `airflow_gate_observations` climbs 6 → 8 → 9; `oldest` pinned at 06:00:00.69 |
| 06:15 | Aborted by hand. Nothing had been recreated, so the `MUTATED=0` branch released intent; generation 18, and the fleet recovered on its own |

Nineteen minutes with every production DAG parked, and it would have hung
indefinitely. `coordination_gate_observations` held no row for generation 17 —
nor for any generation before it. The `INSERT` has never executed once in
production.

## 12. Gaps in this record

Everything Plan 142 Stage 0 item 1 asks for is present: timeline, commands,
failure modes, intended-stopped services, and recovery evidence — all from the
primary record rather than reconstructed.

**Settled, and recorded above rather than outstanding:** the pre-window disk
baseline exists — `/` at 65%, captured at 04:06:23.

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

## 13. Related

- [Plan 142](../plans/plan_142_planned_host_maintenance.md) — the state machine,
  drain contract and stage plan this runbook serves.
- [`maintenance-running-set.txt`](../../maintenance-running-set.txt) — what is
  expected running, what is deliberately stopped, and why.
- [Plan 142 Stage 4](runbook_plan_142_stage_4.md) — the run sheet for the first
  reviewed host window, and the five blockers scoping it found.
- [Storage maintenance](runbook_storage_maintenance.md) — the monthly disk
  check. Its §3 lists the three commands that can destroy this host.
- [Solver OOM and recycle](runbook_solver_oom_and_recycle.md) — `trawl`
  restarts that do *not* need a maintenance window.
