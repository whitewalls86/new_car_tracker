# Runbook: Storage Maintenance

Operational companion to [Plan 135](../plans/plan_135_storage_observability.md). Covers
the monthly disk check, the safe reclaims when something grew, the deploy
sequence for the services that produce these metrics, and the three commands
that can destroy this host.

The point of Plan 135 is that filesystem maintenance is a short, dull,
scheduled task done **from the dashboard**. If the monthly check is clean, it
is five minutes and no SSH session.

---

## 1. The monthly check (no SSH)

Open **Grafana → Cartracker → Infrastructure**.

| Look at | Healthy | Act if |
|---|---|---|
| Filesystem used %, both mounts | `/` under 60%, `/mnt/data` under 70% | either is over, or climbing month over month |
| **Inode used %, `/mnt/data`** | under 50% | over 60% — inodes bind before bytes on this host |
| "What's filling the disks" — root disk panel | bands flat | one band grew |
| "/mnt/data by Docker Volume" | bands flat or stepping weekly | one band grew |

If nothing grew unexpectedly, you are done.

> **Two of these bands step rather than slope, by design.**
> `cartracker_parquet_data` (~4M inodes) and `cartracker_airflow_logs` (~1.2M)
> are walked weekly and carried forward in between, because `du` costs
> O(inodes) — walking them daily took 397s of a 456s run. Read
> `cartracker_disk_usage_measured_timestamp_seconds` to see when each was last
> actually walked. A *frozen* band and a *flat* band look identical without it.

Baselines as of 2026-08-18: `/` at ~64%, `/mnt/data` at 41% bytes / 30% inodes.

---

## 2. A band grew — safe reclaims, in order of safety

| Action | Command | Reclaims | Risk |
|---|---|---|---|
| Truncate a container log | `truncate -s 0 <path>-json.log` | up to 8 GiB | none — loses old stdout only |
| Vacuum journald | `journalctl --vacuum-size=500M` | ~1.2 GiB | none |
| Clear apt cache | `apt-get clean` | ~220 MiB | none |
| Prune dangling images | `docker image prune` | ~0 today | low |
| Prune build cache | `docker builder prune -a -f` | all of it (6.0 GB on 2026-09-08) | low — **and it is automatic now**, see below |

> **This table has been measuring the wrong directory.** `/var/lib/docker` held
> **714 MiB** on 2026-08-29 while `/var/lib/containerd` held **29 GiB** — Docker
> 29 keeps image content in the containerd store, so repeated builds accumulate
> there and `docker image prune` reports "~0 dangling" perfectly truthfully.
> That is most of why `/` went from 49% right after the 2026-08-18 window to
> **72%** eleven days later, against
> [Plan 142](../plans/plan_142_planned_host_maintenance.md)'s 10 GiB
> `disk_headroom` floor. Add `du -xh -d1 /var/lib` to the monthly check.
> `docker system df` does not return within 100 s on this host, which is its own
> signal.

> **Build cache is no longer yours to reclaim by hand.** Decided by
> [Plan 170](../plans/plan_170_container_image_reclaim_policy.md) Stage A and
> shipped by Stage B on 2026-09-08: build cache grew **2.04 → 7.52 GB in 8
> days** while images stayed flat, so it is ~90% of all growth — and it is
> produced only by builds. `scripts/deploy.sh` and `scripts/redeploy.sh` are
> the entire set of things that build on this host, so both now run
> `docker builder prune -a -f` after health verification, non-fatal. Producer
> and reclaim are the same event and no scheduled job exists. **If
> `/var/lib/containerd` is ramping rather than sawtoothing, that is a defect
> in the deploy scripts, not a window you are owed.**
>
> **Nothing in production reads build cache.** It is the saved result of a
> build step, kept so the next build can skip it — a speed optimisation and
> nothing else. Discarding it whole costs the next build its time and costs
> nothing else, and the build runs *before* the deploy-intent drain, so that
> time is not paid in parked DAGs. CI rebuilds every service from cold in
> about 90 seconds.
>
> **There is deliberately no size cap, and that is the second answer.** Stage A
> specified `--keep-storage 4GB`. It was deployed and measured twice on
> 2026-09-08 and enforced nothing: `--keep-storage 4GB -f` took 7.52 → 5.72 GB
> with the cap never reached, and `-a --keep-storage 4GB -f` reclaimed **0 B**
> against 6.00 GB resident. The first is explicable — without `all` the sweep
> skips internal, frontend and shared records — the second is not. No model
> fits both runs, so the policy stopped depending on the flag rather than
> tuning its value. **Do not reintroduce a cap without a measurement showing it
> binds.**

> **Images get no automated retention, and that is a decision.** They do not
> accumulate — 21.03 → 20.97 GB across the same 8 days — and
> `docker image ls -a -f dangling=true` returns **zero**, because Docker 29's
> containerd store drops the old manifest when a tag moves. **The rollback
> window is therefore already zero and no retention rule can raise it**; the
> older wording here, that "rollback depends on previous images being
> present", was measured false on 2026-09-08. `docker system prune -a` stays
> on the §3 list anyway, but for the real reason: it would delete the images
> this host keeps *paused on purpose*, which `maintenance-running-set.txt`
> classifies `aux-paused` and `on-demand` and Docker cannot see.

### The images that must survive any prune

Four of the classes `maintenance-running-set.txt` gives a service — `aux-paused`,
`on-demand`, `profile-running` and `aux-foreign` — mean the same thing to
Docker: **no container holds the image**. So an unreferenced image is not
evidence of anything. Deleting the unreferenced set takes Plan 125's paused
catalog, Plan 112 Gate B's tracking server, both `dbt` tools images, and — in
the window after a reboot, before `--profile trawl` has run — the solver and
its Redis.

**Check an image against this list before deleting it by hand.** It is derived
from `docker-compose*.yml` and `maintenance-running-set.txt` and asserted by
`tests/rules/test_image_keep_set.py`, so it cannot go stale without CI going red. An
image is listed when *no* service that a plain `docker compose up -d` starts
holds it; one running service protects the whole image, which is why
`cartracker-archiver` is absent despite `snapshot-worker` being `on-demand`.

```
# Derived from docker-compose*.yml and maintenance-running-set.txt
# by tests/rules/test_image_keep_set.py, which asserts this block. Do not edit.
on-demand       cartracker-dbt:latest
on-demand       cartracker-dbt_test:latest
on-demand       cartracker-lakehouse:latest
aux-paused      cartracker-mlflow:latest
profile-running ghcr.io/germondai/trawl@sha256:86b1fdf26cfeebd996eeedaeb774002434f4cf8d02e5486f8a8173373c4d6e9b
aux-paused      quay.io/lakekeeper/catalog:v0.13.1
profile-running redis:7-alpine
```

Anything unreferenced and **not** on that list is reclaimable, and is a
reviewed one-off `docker image rm` rather than a prune — the sweep, not a
policy. Images do not accumulate on this host (see above), so there is nothing
here for a schedule to do.

**One more thing reads unreferenced without being garbage, and no derivation
can see it: an image built but not yet started.** A container pins the image
*ID* it started from, not the tag, so a rebuild that does not recreate the
container leaves the new `:latest` held by nothing and the old ID held by a
running container and named by nothing. Both halves were on this host on
2026-09-08 — `cartracker-airflow:latest` at 3.07 GB and
`cartracker-container-health:latest` at 264 MB, freshly built and never
started. **Check the build dates before deleting an image this repo builds**:

```
docker image ls --format '{{.Repository}}:{{.Tag}}\t{{.CreatedSince}}'
docker ps -a -q | xargs -r docker inspect --format '{{.Name}} {{.Image}}'
```

**Truncate, never `rm`, a live log file.** The Docker daemon holds an open
handle; deleting the file frees nothing until the daemon is restarted, and the
space stays invisible to `du` while remaining unavailable.

**Rotation is not retroactive.** Setting `max-size` in `daemon.json` does not
shrink files that are already large. Cap first, then truncate.

---

## 3. What NOT to do on this host

> ### Never run `docker volume prune`
>
> `/var/lib/docker/volumes` is a **symlink to `/mnt/data/docker-volumes`**
> (Plan 105). Docker calls a volume "unused" when no *running* container
> references it, and this host has volumes attached to stopped and
> profile-gated services. A prune here can delete the Postgres data directory,
> the MinIO bronze bucket, or the Loki store. **There is no undo.**

> ### Be careful with `docker system prune -a`
>
> `-a` removes every image not used by a running container, including images
> needed to bring a service back without a rebuild. There are 0 dangling images
> today, so the upside is nil and the downside is a long rebuild — this VM is
> ARM64 (OCI A1.Flex) and rebuilds are slow.
>
> It would also take everything under
> [§2's keep-set](#the-images-that-must-survive-any-prune), which is the real
> reason this stays on the list: those images are unreferenced **by decision**,
> and Docker cannot tell that from garbage.

> ### `docker system df` hangs on this host
>
> Observed 5+ minutes before being killed. Do not reach for it; the dashboard
> panels answer the same question, and `du -s -x --block-size=1` on a specific
> path answers the rest.

---

## 4. Deploying the services that produce these metrics

The disk watchlist runs on **pack-worker**, which shares the
`cartracker-archiver` image tag with `archiver` (`archiver/Dockerfile` does
`COPY . .`, so processor and app code is **baked into the image**, not mounted).

**A bare `docker compose up -d` restarts pack-worker on the old image.**
`/disk-usage/run` then 404s while every container looks healthy and recently
started. The correct sequence:

```bash
git pull
docker compose build archiver                  # shared tag; covers pack-worker
docker compose up -d node-exporter pack-worker archiver

# Verify what LOADED, never uptime:
docker exec cartracker-pack-worker ls /app/archiver/processors/disk_usage.py
docker inspect cartracker-node-exporter --format '{{json .Args}}'
```

### Restart vs. recreate — they are exact opposites

This bit twice in one evening. Neither command is the safe general answer.

| Change | `docker restart` | `docker compose up -d` |
|---|---|---|
| compose `command:` / `volumes:` / `environment:` | **ignored** — reuses the existing container config | applies |
| contents of a **bind-mounted file** | applies | **ignored** — compose sees no config drift |
| new code in a rebuilt image | ignored | applies, but only after `build` |

Confirm with `docker inspect --format '{{json .Args}}'` or
`--format '{{json .Config.Env}}'`. **Container uptime is not evidence a deploy
landed.**

### The deploy-intent window

Every DAG's `deploy_intent_sensor` has `timeout=600`. **Build before declaring
deploy intent** and keep the window under 10 minutes, or the first task of every
DAG that starts during it fails.

---

## 5. Fire-testing an alert

When verifying a Grafana alert actually delivers, **hold the test condition
past `group_wait` (30s)**. A rule that fires and resolves inside that window
cancels its own notification, and the alert reads as broken when it is working.

---

## 6. Related runbooks

- [runbook_plan_131_stage_3_4.md](runbook_plan_131_stage_3_4.md) — packing and
  pruning bronze HTML, the largest inode lever on `/mnt/data`.
- [runbook_solver_oom_and_recycle.md](runbook_solver_oom_and_recycle.md) —
  `trawl` OOM evidence and recycling.

---

## 7. Stage 5 log-retention rollout

This is a scheduled production change, not part of the routine monthly check.
It combines Docker's bounded stdout buffer with Promtail ingestion so the
`oauth2-proxy` audit trail is never made disposable before it has a durable
copy. It also enables Loki's 90-day retention, which irreversibly deletes older
data on compaction.

### Preflight and explicit decisions

Before the window:

1. Confirm Loki data older than 90 days is expendable.
2. Confirm losing the existing Docker stdout backlog is acceptable. Rotation
   is not retroactive; old files must be truncated separately.
3. Build any changed images before declaring deploy intent. The Stage 5 files
   are bind-mounted or host configuration and normally need no image build.
4. Record `df -h /`, `df -h /mnt/data`, `df -i /mnt/data`,
   `journalctl --disk-usage`, and `du -sb /var/lib/docker/containers`.

Stage 5's 2026-08-17 preflight found `/` 65%, `/mnt/data` 34%, data-volume
inodes 17%, Docker stdout 5.44 GB, and journald 1.8 GB. The four stdout streams
selected for Loki were also the four largest: Airflow DAG processor 2.22 GB,
`oauth2-proxy` 1.65 GB, Airflow scheduler 752 MB, and Airflow API server 299 MB.

### Apply the Docker buffer and recreate containers

Create `/etc/docker/daemon.json` as:

```json
{
  "log-driver": "json-file",
  "log-opts": {"max-size": "50m", "max-file": "3"}
}
```

Validate before restarting:

```bash
sudo dockerd --validate --config-file=/etc/docker/daemon.json
```

Then, inside the planned outage window:

```bash
curl -sf -X POST http://localhost:8060/deploy/start
sudo systemctl restart docker
cd /opt/cartracker
docker compose up -d --force-recreate
curl -sf -X POST http://localhost:8060/deploy/complete
```

The forced recreation is essential: daemon defaults apply only to newly
created containers. A daemon restart alone leaves existing containers' old
unbounded logging configuration in place.

Verify every running container loaded the cap, not merely that it restarted:

```bash
docker inspect $(docker ps -q) \
  --format '{{.Name}} {{.HostConfig.LogConfig.Type}} {{json .HostConfig.LogConfig.Config}}'
```

Every line should show `json-file`, `max-size: 50m`, and `max-file: 3`.

### Verify durable ingestion before truncating stdout

Promtail must show no discovery, permission, or parse errors. Query Loki for
fresh entries from all four selected services:

```bash
docker logs --since 10m cartracker-promtail
curl -sG http://localhost:3100/loki/api/v1/query \
  --data-urlencode 'query=sum by (service) (count_over_time({service=~"oauth2-proxy|airflow-(dag-processor|scheduler|apiserver)"}[10m]))'
```

Do not truncate until `oauth2-proxy` and the Airflow control-plane streams have
appeared in Loki. Once verified, truncate the current files in place — never
remove them:

```bash
for id in $(docker ps -q); do
  path=$(docker inspect --format '{{.LogPath}}' "$id")
  sudo truncate -s 0 "$path"
done
```

### Apply the remaining caps

Loki retention is loaded only after Loki restarts. Verify readiness and the
effective configuration after recreation:

```bash
curl -sf http://localhost:3100/ready
curl -sf http://localhost:3100/config | grep -A3 -E 'retention_(enabled|period)'
```

The `prune_task_logs` DAG deletes only `dag_id=*/run_id=*` trees whose newest
log file is older than 30 days. Unpause it and trigger its first run manually;
inspect the task result before relying on the weekly schedule:

```bash
docker exec cartracker-airflow-apiserver airflow dags unpause prune_task_logs
docker exec cartracker-airflow-apiserver airflow dags trigger prune_task_logs
```

Finally cap and vacuum journald:

```bash
sudo mkdir -p /etc/systemd/journald.conf.d
printf '[Journal]\nSystemMaxUse=500M\n' | \
  sudo tee /etc/systemd/journald.conf.d/plan135-storage.conf
sudo systemctl restart systemd-journald
sudo journalctl --vacuum-size=500M
```

Re-run the preflight measurements and record the reclaimed bytes/inodes in
[Plan 135](../plans/plan_135_storage_observability.md).
