# Plan 135: Storage Observability — The Data Volume Is Not Monitored

## Status

PLANNED, with **Stage 5b-bis partially applied 2026-08-14**. Written after a
question about why the MinIO storage gauge reads ~61 GiB while `df /mnt/data`
reads ~97 GiB. Answering that question surfaced a defect: **`/mnt/data` has
never been monitored at all.** The plan starts with that bug, then adds the
panels and alerts that would have made it visible.

### Applied 2026-08-14 (Stage 5b-bis)

| Action | Result |
|---|---|
| `POST /log_level?log_level=warn` on Loki | verified: `{"status":"success","message":"Log level set to warn"}` |
| Write rate after change | **0 bytes in 20s**, down from ~9 lines/sec |
| `truncate -s 0` on Loki's json log | 8,611,107,005 → 0 bytes |
| `df /` | **79% → 63%** (11 GiB → 19 GiB free) |
| Loki health after | `/ready` 200; ingestion verified live end-to-end (promtail sent +368 = distributor received +368 over 40s) |

`log_level: warn` was also added to [loki/loki.yml](loki/loki.yml) so the change
survives a restart, but that is **not yet deployed** — it reaches production via
commit/push/pull and takes effect at Loki's next restart. Until then the runtime
setting is what is holding, and it would revert if the container restarted.

Everything else in this plan is still outstanding.

### Inherited scope: Plan 131 Stage 5 Step 6 (2026-08-17)

[Plan 131](plan_131_packed_cold_storage.md)'s Stage 5 specified its own inode
alerts — its Step 6, called there "the highest-value item in the stage," on a
plan whose premise is that bytes improved while the inode clock did not slow.
**That work is now this plan's Stage 3**, for two reasons:

- It is the same two rules in the same file. Two plans editing
  `rules.yml`'s `infra-and-data-health` group is how you get divergent
  thresholds and a merge conflict.
- **It cannot work without Stage 1.** Plan 131's spec assumed "node-exporter is
  already scraped and already exports what is needed." It does not export
  `/mnt/data` at all, so an inode rule written there would have evaluated `/` —
  9% used — while the volume both plans exist to protect sits at 61% and
  invisible. The prerequisite lives here, so the alert should too.

Plan 131 keeps its verification-failure alert, which is pack integrity rather
than disk capacity. This plan does not inherit that.

**Consequence for sequencing:** Plan 131's closeout gate reads "measured, and
alertable." The inode half of that is now satisfied by this plan's Stage 3, so
Plan 135 Stage 1 + Stage 3 should land before Plan 131 is called complete.

**Verification note:** BusyBox `wget` inside the Prometheus container reported
`/ready` as 503; `curl` from the host reported 200 with body `ready`. The host
`curl` is the trustworthy reading — Loki's port is published, so prefer it over
`docker exec ... wget` for health checks.

## Goal

Make the storage picture answerable from a dashboard instead of an SSH session,
**for both disks**, and make routine filesystem maintenance a scheduled habit
rather than an emergency:

1. `/` and `/mnt/data` capacity and inode headroom are scraped, charted, alerted.
2. The logical/physical gap for MinIO is a visible number, not a surprise.
3. Inode exhaustion — the constraint that actually binds first — has an alert.
4. **What is filling each disk** is visible at the per-volume and per-path level,
   without SSH and without a filesystem-explorer GUI.
5. Unbounded log growth is bounded, and there is a short runbook for the
   recurring maintenance chores.

### The two disks

| | `/` (`/dev/sda1`) | `/mnt/data` (`/dev/sdb`) |
|---|---|---|
| Size | 48.3 GiB | 195.8 GiB |
| Used | 38.0 GiB (**79%**) | 97.4 GiB (52%) |
| Holds | OS, Docker images/layers/build cache | all Docker volumes |
| Monitored today | bytes only | **nothing** |

Note `/var/lib/docker/volumes` is a symlink to `/mnt/data/docker-volumes`
(Plan 105), so Docker's *volume* data lives on `sdb` while its *image and layer*
data lives on `sda1`. Any tooling here must keep those two straight, and any
prune command must be evaluated against both.

## Context

### The reported symptom

The `MinIO Storage Used (bytes)` panel on the Infrastructure dashboard queries
`minio_bucket_usage_total_bytes`. That metric is the sum of **object payload
sizes** in `bronze` and nothing else. It is not a disk-usage metric.

Measured on the production VM, 2026-08-14 17:20 UTC:

| Measure | Value |
|---|---|
| `minio_bucket_usage_total_bytes{bucket="bronze"}` | 65,491,733,596 B = **61.0 GiB** |
| `df /mnt/data` used | **97.4 GiB** of 195.8 GiB (52%) |
| `df -i /mnt/data` used | **7,965,523** of 13,107,200 inodes (61%) |
| `.minio.sys` total | 87 MB |
| `minio_cluster_usage_version_total` | 0 |
| `minio_cluster_usage_deletemarker_total` | 0 |

The gap is not scanner staleness (the scanner's `.usage.json` was written 4
minutes before the reading), not object versions, not delete markers, and not
orphaned multipart uploads (`.minio.sys/multipart` is empty). It is the
per-object storage floor plus the non-MinIO volumes sharing the mount.

### Where the 36.4 GiB gap actually goes

Measured per Docker volume, 2026-08-14:

| Volume | Size | Note |
|---|---|---|
| `cartracker_loki_data` | 4.54 GiB | **no retention configured** |
| `cartracker_analytics_db` | 3.71 GiB | |
| `cartracker_airflow_logs` | ~2.9 GiB | **no retention configured**; ~110k runs |
| `cartracker_prometheus_data` | 1.65 GiB | 30d retention, working as intended |
| `cartracker_pgdata` | 1.61 GiB | |
| everything else | <0.2 GiB | grafana, lakekeeper, caddy, redis, pgadmin |
| `cartracker_raw` | 0.00 GiB | legacy pre-MinIO volume, empty |
| **non-MinIO total** | **~15 GiB** | |

So the accounting closes as:

| Component | Size | Share of gap |
|---|---|---|
| MinIO logical (the gauge) | 61.0 GiB | — |
| MinIO per-object floor | ~21 GiB | ~58% |
| Non-MinIO volumes | ~15 GiB | ~42% |
| **Mount used** | **97.4 GiB** | |

MinIO's physical footprint is therefore ~82 GiB against 61.0 GiB logical — a
**~1.35× amplification**, consistent with the sampled per-object measurements
below. This is the number Stage 2's amplification panel should reproduce.

### The per-object floor, measured

MinIO stores each object as a *directory* containing an `xl.meta` file, with the
payload inlined below 128 KB. On a 4 KiB-block ext4 single-drive backend that
costs 4 KiB for the directory plus the 4 KiB-rounded `xl.meta`.

Sampled directly with `find -printf '%s %b'`:

| Partition | Objects | Mean apparent | Mean physical | Tax |
|---|---|---|---|---|
| `html/year=2026/month=8/artifact_type=results_page` | 3,288 (full) | 37.4 KiB | 43.1 KiB | +15.5% |
| `html/year=2026/month=8/artifact_type=detail_page` | 80,000 (sample) | 31.7 KiB | 37.8 KiB | +19.0% |

Those are the *large*-object partitions. The bucket-wide size histogram shows
where the mass actually sits:

| Range | Objects |
|---|---|
| `LESS_THAN_1024_B` | 869 |
| `BETWEEN_1024_B_AND_64_KB` | **3,519,164** |
| `BETWEEN_64_KB_AND_256_KB` | 6,882 |
| everything ≥ 256 KB | 1,892 |

**99.7% of objects are between 1 KB and 64 KB.** The floor is a fixed ~4–6 KiB
per object, so the smaller the mean object, the worse the ratio. Plan 129's
dictionary compression cut logical bytes hard, which *raised* the relative tax —
compression makes the small-file problem proportionally worse, not better.

Note a discrepancy worth resolving during implementation:
`minio_bucket_usage_object_total` reports 4,085,872 while the size histogram
sums to 3,528,807. Inode arithmetic (2 inodes per object) favours the histogram:
3,528,807 × 2 ≈ 7.06M of the 7.97M inodes used. The two metrics come from
different scanner bookkeeping; the panel should not silently pick one.

### The defect: `/mnt/data` is invisible to Prometheus

`node-exporter` reports filesystem metrics for **`/` only**:

```
node_filesystem_size_bytes{device="/dev/sda1",mountpoint="/"} 5.18e+10
```

There is no `/mnt/data` series. The reason is explicit in the metrics:

```
node_filesystem_device_error{device="/dev/sdb",fstype="ext4",
  mountpoint="/mnt/data",device_error="no such file or directory"} 1
```

In [docker-compose.yml:599-613](docker-compose.yml#L599-L613), node-exporter
bind-mounts the host root at `/rootfs` but never passes `--path.rootfs=/rootfs`.
So it reads the *host* mount table via `--path.procfs=/host/proc` (and correctly
sees `/dev/sdb` at `/mnt/data`), then tries to `statfs("/mnt/data")` inside its
own namespace, where `/mnt` does not exist.

The consequences:

- **Both disk alerts are pointed at the wrong disk.** `ct-disk-space-warning`
  and `ct-disk-space-critical` in
  [rules.yml:310-380](grafana/provisioning/alerting/rules.yml#L310-L380) match
  `node_filesystem_avail_bytes{fstype!="tmpfs",mountpoint!~"/boot.*"}`, which
  resolves to `/` alone. The 196 GB data volume — the one that filled to
  99–100% during Plan 129's backfill and again during Plan 114's audit — has
  never had capacity alerting.
- **The Infrastructure dashboard has no filesystem capacity panel at all.** It
  charts Disk I/O (`node_disk_*_bytes_total`) but not free space or inodes.
- **Nothing tracks inodes anywhere**, on either volume.

Verified as safe to fix: Docker's bind mount is recursive, so
`/rootfs/mnt/data` inside the container is genuinely `/dev/sdb`
(`df` there reports 195.8G / 13,107,200 inodes, matching the host). Setting
`--path.rootfs=/rootfs` will report real numbers, not a silent `/` passthrough.

### The root disk: 79% full, and it is almost entirely container logs

`/` is at **79%** (39 of 49 GiB, 11 GiB free) — one point below the warning
threshold. Unlike `/mnt/data` it is *not* inode-constrained (9% inodes used), so
root is purely a bytes problem. Measured with `du -x` (cheap here — only 558k
inodes on this filesystem):

| Path | Size | Verdict |
|---|---|---|
| `/var/lib/containerd` | 21 GiB | image layers (16 GiB snapshots + 4.7 GiB blobs) |
| **`/var/lib/docker/containers`** | **13 GiB** | **container stdout logs — no rotation** |
| `/var/log/journal` | 1.7 GiB | journald, no cap configured |
| `/usr` | 1.8 GiB | OS |
| `/var/lib/snapd` | 749 MiB | |
| `/var/cache/apt` | 218 MiB | trivially reclaimable |
| `/var/lib/docker/buildkit` | 6.8 MiB | negligible — not the problem |

Two things stand out.

**The images are legitimate.** 30 images, **0 dangling**, and the daemon uses the
containerd image store (`Storage Driver: overlayfs`, so layers live under
`/var/lib/containerd`, not `/var/lib/docker/overlay2`). There is no pile of
stale images to prune here — the 21 GiB is the running stack. Note that
`docker system df` **hung for 5+ minutes** and had to be killed; it is not a
usable tool on this host, which is part of why this went unnoticed.

**Docker's own container logs are the single largest reclaimable item on the
box — 12.31 GiB across 31 files, with no rotation configured anywhere.** There
is no `logging:` block in [docker-compose.yml](docker-compose.yml), no
`/etc/docker/daemon.json`, and no journald cap. The default `json-file` driver
grows without bound.

| Container | stdout log |
|---|---|
| **`cartracker-loki`** | **8.02 GiB** |
| `cartracker-airflow-dag-processor` | 1.82 GiB |
| `cartracker-oauth2-proxy` | 1.40 GiB |
| `cartracker-airflow-scheduler` | 0.62 GiB |
| `cartracker-airflow-apiserver` | 0.24 GiB |
| all others | <0.1 GiB each |

The largest log file on the server is **Loki's own stdout** — the log
aggregator's chatter, captured by Docker's json driver, sitting on disk while
Promtail also ships container logs *into* Loki. 8 GiB of logs about logging.

This reframes the retention question. The Airflow task logs (~2.9 GiB) and the
Loki chunk store (4.5 GiB) on `/mnt/data` are real but second-order; the
unbounded thing is on the *other* disk, and neither disk was being watched.

## Design

### Why physical size cannot be a scrape-time metric

Getting MinIO's true on-disk size means walking ~8M inodes. A `du -sb` over the
volume ran for **20+ minutes without completing** while this plan was being
written, and it contends with live scraper I/O. Physical size must be sampled by
a scheduled job, never computed during a Prometheus scrape.

Two tiers, in order of cost:

- **Free and continuous:** once `/mnt/data` is scraped,
  `node_filesystem_size_bytes - node_filesystem_avail_bytes` gives mount-level
  used bytes every 15s. Charted against `minio_bucket_usage_total_bytes`, the
  ratio between the two lines *is* the amplification factor. It folds in the
  non-MinIO volumes, but those are small and the trend is what matters.
- **Exact and daily:** a scheduled `du` per Docker volume, published through
  node-exporter's textfile collector. This is the only way to separate "MinIO
  physical" from "Postgres + Loki + Prometheus + logs", and it is worth having
  once, then daily, not on demand.

### The log architecture: two streams, not two copies

It is tempting to describe this system as storing every log twice and to reach
for "prune one, archive the other". Measurement says otherwise. There are two
*separate* streams with almost no overlap, and they have opposite problems.

**Stream A — application file logs → Loki.** [promtail.yml](promtail/promtail.yml)
scrapes `app.log*` from five volumes (`ops`, `scraper`, `processing`,
`dbt_runner`, `archiver`). It does **not** mount `/var/lib/docker/containers`.

| | |
|---|---|
| On-disk buffer (5 volumes) | **0.117 GiB total** |
| Rotation | already working — `app.log` + `.1 .2 .3`, 5 MB each (~20 MB/service) |
| Durable copy | Loki, 4.54 GiB spanning 2026-05-01 → 2026-08-14 (105 days) |
| Ingest rate | ~44 MB/day |

**This stream is already correct.** The file is a small bounded buffer, Loki is
the durable store, and the "duplicate" costs 117 MB — a rounding error. There is
nothing here worth optimising.

**Stream B — container stdout → nowhere.** Docker's `json-file` driver captures
all 31 containers to `/var/lib/docker/containers`, and **Promtail never reads
it**.

| | |
|---|---|
| On-disk | **12.31 GiB, unrotated** |
| Durable copy | **none — this is the only copy** |
| Searchable in Grafana | **no** |

So the storage is not going to a redundant second copy. It is going to a stream
that has *no* second copy and *no* retention, and which includes the logs you
would actually want during an incident — `airflow-dag-processor` (1.82 GiB),
`oauth2-proxy` (1.40 GiB), `airflow-scheduler` (0.62 GiB) — none of which are
queryable today.

### The principle: buffer vs. store

The fix is not to pick a copy to delete. It is to give each stream one job:

- **The file on disk is a transport buffer, not a store.** Size it in hours, cap
  it, never think about it again. Stream A already does this; Stream B does not.
- **Loki is the single durable store.** Retention lives there, in exactly one
  place, and the data is compressed.

This directly answers the "adding observability makes both explode together"
worry. Once the buffer is capped, its total is bounded by
`containers × max-size × max-file` **regardless of how chatty anything gets** —
a new noisy service raises Loki's ingest, not the disk buffer. Growth becomes
one-dimensional and one knob controls it.

The corollary is that the per-container decision is no longer "keep or delete"
but **"does this belong in Loki at all?"**, which is a content question:

- `cartracker-loki`'s own 8.02 GiB of stdout is the log aggregator narrating
  itself. Nobody reads it, and shipping it into Loki risks a feedback loop.
  Cap it hard, lower its log level, never ingest it.
- `airflow-*` and `oauth2-proxy` are operationally interesting and currently
  invisible. They should arguably be *added* to Loki. At their observed rates
  (~15 and ~11 MB/day) that lifts Loki from ~44 to ~70 MB/day.

### On archiving: probably don't

At ~44 MB/day (or ~70 with the containers above added), 90-day retention is
~4–6 GiB steady state. That is already cheap enough that an archive tier is
mostly ceremony.

More importantly, **archiving to the obvious target buys nothing**. Loki
supports an S3 backend and this project runs MinIO — but MinIO lives on
`/mnt/data`, the same physical volume Loki already occupies. Moving chunks there
relocates bytes sideways and adds objects to the volume that is *inode*-
constrained. A genuine archive would have to leave the box entirely (OCI Object
Storage), which is a real cost and a real egress path for data that, in
practice, is almost never read.

Recommendation: pick a retention you can defend, let data older than that go,
and revisit only when a concrete need to read 6-month-old logs appears. If that
need does appear, the right move is a targeted export of the specific query
result, not a standing archive of everything.

### Inodes are the binding constraint

Inodes are at 61% while bytes are at 52%, and each new object costs 2 inodes at
a mean payload of ~16 KB. Plan 114 already recorded this ("inode headroom is
tighter than byte headroom") and it has still never been alerted on. Inode
exhaustion presents as `ENOSPC` with free space on the disk — a confusing
failure worth catching early. A `predict_linear` alert on time-to-exhaustion is
more useful here than a static threshold, because the fill rate is what changes
when a backfill starts.

## Stages

### Stage 1 — Make `/mnt/data` visible (the actual bug)

Add the missing flag in [docker-compose.yml](docker-compose.yml#L608):

```yaml
    command:
      - '--path.procfs=/host/proc'
      - '--path.sysfs=/host/sys'
      - '--path.rootfs=/rootfs'
      - '--collector.filesystem.mount-points-exclude=^/(sys|proc|dev|host|etc)($$|/)'
```

Verify:

- `node_filesystem_device_error{mountpoint="/mnt/data"}` reads `0`.
- `node_filesystem_size_bytes{mountpoint="/mnt/data"}` ≈ 2.10e+11.
- `node_filesystem_files{mountpoint="/mnt/data"}` = 13107200.
- The `mountpoint="/"` series still exists and did not gain a `/rootfs` prefix
  (node-exporter strips the rootfs prefix from the label; confirm rather than
  assume, since the two disk alerts match on `mountpoint`).

Restarting node-exporter is a production change and needs confirmation before it
is applied.

### Stage 2 — Dashboard panels

Add to [infrastructure.json](grafana/dashboards/infrastructure.json), which
currently ends at panel id 8:

1. **Filesystem Used %** — `(1 - node_filesystem_avail_bytes / node_filesystem_size_bytes)`,
   legend `{{mountpoint}}`, unit `percentunit`, thresholds at 0.8 / 0.9. Both
   volumes on one panel.
2. **Inode Used %** — `(1 - node_filesystem_files_free / node_filesystem_files)`,
   same shape and thresholds. This is the panel that would have shown the 61%.
3. **MinIO Logical vs Mount Physical** — two series, `minio_bucket_usage_total_bytes`
   and `node_filesystem_size_bytes{mountpoint="/mnt/data"} - node_filesystem_avail_bytes{mountpoint="/mnt/data"}`,
   unit `bytes`. The visible divergence is the point of the panel.
4. **Storage Amplification Ratio** — the quotient of the two above, unit `none`.
   A single number that answers this plan's originating question at a glance.
5. **Mean Object Size** — `minio_bucket_usage_total_bytes / minio_bucket_usage_object_total`,
   unit `bytes`. Falling mean = rising tax; this is the leading indicator for
   whether packing (Plan 131) is earning its keep.

Retitle panel 6 from `MinIO Storage Used (bytes)` to **`MinIO Logical Object
Bytes`**. The current title is what made the number look like a disk reading.

### Stage 3 — Alerts

In [rules.yml](grafana/provisioning/alerting/rules.yml), group
`infra-and-data-health`:

- **Scope the two existing disk alerts.** After Stage 1 they will start matching
  `/mnt/data` as well as `/`. Confirm the intent is both volumes; if per-volume
  thresholds are wanted, split the rules. This must be checked *with* Stage 1 —
  Stage 1 alone silently widens what those alerts cover.
- **`ct-inode-warning`** (>80%) and **`ct-inode-critical`** (>90%) on
  `(1 - node_filesystem_files_free / node_filesystem_files) * 100`, mirroring the
  existing disk rules' shape and `for:` durations. Match the same
  `fstype!="tmpfs",mountpoint!~"/boot.*"` selectors the byte rules use, so the
  two families cover the same filesystems. **This is also
  [Plan 131](plan_131_packed_cold_storage.md)'s Stage 5 Step 6** — see
  [inherited scope](#inherited-scope-plan-131-stage-5-step-6-2026-08-17).
  `tests/test_observability_config.py` already validates this file; confirm it
  covers the new rules rather than assuming, and check the rendered expression
  returns data against the live Prometheus before calling it done.
- **`ct-inode-exhaustion-forecast`** — `predict_linear(node_filesystem_files_free{mountpoint="/mnt/data"}[6h], 7*24*3600) < 0`,
  `for: 1h`. Fires when the current rate would exhaust inodes within a week.
  This is the rule that catches a runaway backfill while there is still time.

Annotations should name the mountpoint and link to the Infrastructure dashboard,
matching `ct-pipeline-failures`.

### Stage 4 — "What is filling the disk", for both disks

This is the stage that replaces the SSH session. It is a **fixed watchlist of
paths**, not a filesystem explorer: roughly 20 series, chosen once, that between
them account for ~95% of both disks. When a disk grows, one line on the graph
grows, and that is the answer.

Mechanism: node-exporter's textfile collector plus one scheduled job.

- Enable `--collector.textfile.directory=/textfile` on node-exporter, with a
  writable bind mount for the directory.
- A daily job writes a `.prom` file **atomically** — write to a temp file, then
  `rename()`. node-exporter reads partial files and will emit garbage otherwise.
- Emit two metric families:

```
cartracker_path_bytes{disk="root",  path="/var/lib/containerd"}
cartracker_path_bytes{disk="root",  path="/var/lib/docker/containers"}
cartracker_path_bytes{disk="root",  path="/var/log/journal"}
cartracker_path_bytes{disk="root",  path="/usr"}
cartracker_path_bytes{disk="root",  path="/var/lib/snapd"}
cartracker_volume_bytes{volume="cartracker_parquet_data"}     # MinIO
cartracker_volume_bytes{volume="cartracker_loki_data"}
cartracker_volume_bytes{volume="cartracker_analytics_db"}
cartracker_volume_bytes{volume="cartracker_airflow_logs"}
cartracker_volume_bytes{volume="cartracker_pgdata"}
cartracker_volume_bytes{volume="cartracker_prometheus_data"}
```

Cost discipline, which matters because the naive version of this job is a
20-minute disk thrash:

- **Root disk:** `du -x --max-depth=1` is cheap here (558k inodes total, `-x`
  stops at the filesystem boundary). Measure it properly, daily.
- **`/mnt/data`:** ~8M inodes. Only `cartracker_parquet_data` is expensive; every
  other volume is small and fast. Walk the small ones daily; walk the MinIO
  volume **weekly**, off-peak, and carry the last value forward between runs.
- Do **not** call `docker system df` from the job. It hung for 5+ minutes on
  this host and had to be killed.

Add a dashboard row **"What's Filling The Disks"**: two stacked-area panels
(one per disk) over these series, plus a table panel of the current top ten
sorted descending. Stacked area is the right form — the question is always
"which band grew", and the total should visibly reconcile with the `df` line
from Stage 2.

The derived metric worth alerting on later is
`cartracker_volume_bytes{volume="cartracker_parquet_data"} / minio_bucket_usage_total_bytes`
— the true MinIO amplification factor, with non-MinIO volumes excluded.
Today that number is ~1.35.

### Stage 5 — Give each stream one job

Applying the buffer/store split above. Note that Stream A (app file logs →
Loki) needs **no changes** — it already works. Everything below is Stream B,
plus the one retention knob Loki is missing.

**5a. Cap the transport buffer — 12.31 GiB, the big one.** Set a global default
in `/etc/docker/daemon.json` so it applies to every container including ones
added later:

```json
{
  "log-driver": "json-file",
  "log-opts": { "max-size": "50m", "max-file": "3" }
}
```

That caps each container at 150 MB, so ~31 containers × 150 MB ≈ 4.6 GB worst
case, against 12.31 GiB today. Requires a Docker daemon restart, which restarts
containers — schedule it. **Rotation only applies to containers created after
the change**; existing logs must be truncated separately (see the runbook).

Consider whether `cartracker-loki` needs stdout capture at all — 8.02 GiB of the
12.31 is Loki logging about logging, while Promtail ships container logs *into*
Loki. Reducing Loki's own log level is likely a bigger win than rotating it.

**5b. Set the one retention knob — Loki.** [loki/loki.yml](loki/loki.yml) has no
compactor and no retention period, so chunks are kept forever. Prometheus
already does this correctly (`--storage.tsdb.retention.time=30d` at
[docker-compose.yml:625](docker-compose.yml#L625)); Loki needs the same:

```yaml
compactor:
  working_directory: /loki/compactor
  retention_enabled: true
  delete_request_store: filesystem

limits_config:
  retention_period: 90d
```

90d rather than 30d, deliberately. At ~44 MB/day that is ~4 GiB steady state —
affordable — and it is the *only* durable log store, so it should be generous.
Current data spans 105 days, so enabling 90d retention deletes roughly the
oldest two weeks on first compaction. Confirm that is acceptable before enabling.

**5b-bis. Turn Loki's own log level down — the primary fix for 8.6 GiB.**

Rotation caps the file; the log level removes the writes. Measured on the live
container:

| | |
|---|---|
| Size | 8,610,290,177 bytes (8.6 GB) |
| Rate | 200,000 lines / 6 hours ≈ **9 lines/sec, sustained** |
| Composition | 52% `caller=index_set.go`, 36% `caller=table.go` — boltdb-shipper index handover |
| Levels in last 2M lines | **1,999,998 `info` / 2 `error` / 0 `warn`** |

**Two actionable lines out of two million.** The signal is 0.0001% of the
volume, and both errors were `context canceled` / `EOF` from one query-scheduler
blip on 2026-08-13 — arguably not actionable either.

Set `server: log_level: warn` in [loki/loki.yml](loki/loki.yml).

**This is safe specifically because the level is a runtime dial, not a permanent
setting.** Verified on the running instance (Loki 2.9.8):

```
GET http://loki:3100/log_level  →  {"message":"Current log level is info"}
```

The companion `POST /log_level?log_level=<level>` changes it **without a
restart** — the query-parameter form is verified working (an empty POST body is
fine). So the troubleshooting workflow for a Loki problem that never surfaces as
an error is:

1. `POST /log_level?log_level=debug`
2. `docker logs -f cartracker-loki` — or just let the json buffer collect it
3. reproduce the problem; at ~9 lines/sec you have thousands of lines in minutes
4. `POST /log_level?log_level=warn`

Two useful properties fall out. Runtime changes **revert on container restart**,
so the configured `warn` is the fail-safe default — you cannot accidentally
leave it verbose forever. And with the buffer capped at 150 MB against info's
~76 MB/day, the ring holds roughly **two days** of info-level history, so a
debug session is captured without any extra plumbing.

The validation that matters: the `error` channel is **not** silent — Loki does
emit at that level when something genuinely goes wrong, so `warn` is a real
filter rather than a mute button.

**5c. Decide what belongs in Loki.** Once the buffer is capped, container stdout
older than the buffer window is gone forever. For most containers that is fine.
For a few it is not, and those should be added as Promtail scrape jobs so the
durable copy exists where it is queryable:

| Container | Rate | Recommendation |
|---|---|---|
| `cartracker-loki` | ~76 MB/day | **never ingest** — feedback-loop risk, and 5b-bis removes the volume at source |
| `airflow-dag-processor` | ~15 MB/day | add to Loki |
| `oauth2-proxy` | ~11 MB/day | add to Loki — it is the auth audit trail |
| `airflow-scheduler` / `apiserver` | ~7 MB/day | add to Loki |
| everything else | negligible | buffer only |

Adding those lifts Loki from ~44 to ~70 MB/day, or ~6 GiB at 90d retention.
Promtail will need `/var/lib/docker/containers` mounted read-only and a
`docker_sd_configs` (or static path) scrape job with a `cri`/`docker` pipeline
stage. Note this is the step that *increases* one number in order to make the
system honest — the logs exist either way; this puts them somewhere useful and
bounded instead of somewhere unbounded and unsearchable.

**5d. Airflow task logs — ~2.9 GiB but ~1.2M inodes.** This is an *inode* fix,
not a space fix, and inodes are the binding constraint on `/mnt/data`. Logs go
back to 2026-04-20 with nothing deleting them; at 11 inodes per run across
~110k runs it is roughly 15% of the mount's inode usage for under 3 GiB of data.

Airflow has native support — set `AIRFLOW__LOG_RETENTION_DAYS` (or add a
maintenance DAG that prunes `dag_id=*/run_id=*` older than N days). 30 days
matches Prometheus and Loki. Verify the retention variable is honoured by the
Airflow version in use rather than assuming; if not, the maintenance DAG is the
fallback.

**5e. journald — 1.7 GiB, uncapped.** One line in `/etc/systemd/journald.conf`:

```
SystemMaxUse=500M
```

Expected recovery once these land: **~15 GiB on `/` (79% → ~48%)** and ~1.2M
inodes on `/mnt/data`, against a Loki store that grows to ~6 GiB and then stops.

**What explicitly does not change:** Stream A. The five app log volumes total
117 MB and already rotate correctly. Leave them alone.

### Stage 6 — The maintenance runbook

The point of this plan is that filesystem maintenance becomes a short, dull,
scheduled task. Write `docs/runbook_storage_maintenance.md` covering:

**Monthly, from the dashboard (no SSH):** check both disks' used %, check inode
% on `/mnt/data`, check the "What's Filling The Disks" panel for a band that
grew. If nothing grew unexpectedly, done.

**When a band did grow — safe reclaims, in order of safety:**

| Action | Command | Reclaims | Risk |
|---|---|---|---|
| Truncate a container log | `truncate -s 0 <path>-json.log` | up to 8 GiB | none (loses old stdout only) |
| Vacuum journald | `journalctl --vacuum-size=500M` | ~1.2 GiB | none |
| Clear apt cache | `apt-get clean` | ~220 MiB | none |
| Prune dangling images | `docker image prune` | 0 today | low |
| Prune build cache | `docker builder prune` | ~7 MiB today | low |

**What NOT to do on this host — the sharp edge:**

> **Never run `docker volume prune`.** `/var/lib/docker/volumes` is a symlink to
> `/mnt/data/docker-volumes` (Plan 105). Docker considers a volume "unused" if no
> *running container* references it, and this host has volumes attached to
> stopped/occasional services. A volume prune here can delete Postgres data, the
> MinIO bronze bucket, or the Loki store. There is no undo.

> **Be careful with `docker system prune -a`.** The `-a` flag removes all images
> not used by a running container, including ones needed to bring services back
> up without a rebuild. There are 0 dangling images today, so the upside is nil
> and the downside is a long rebuild on ARM64.

Record in the runbook that `docker system df` hangs on this host and should not
be reached for.

## Success Criteria

1. `node_filesystem_device_error{mountpoint="/mnt/data"}` is `0` and both bytes
   and inode series are present for `/dev/sdb`.
2. The Infrastructure dashboard shows filesystem %, inode %, and the
   logical-vs-physical divergence for both volumes.
3. Inode alerts exist and have been shown to fire (test by lowering the
   threshold temporarily, not by filling the disk).
4. The originating question — "why 61 vs 97?" — is answerable from the
   dashboard without SSH.
5. "What is filling `/`?" and "what is filling `/mnt/data`?" are each answerable
   from one panel, without SSH.
6. No log source grows without bound: Docker json-file capped, Loki retention
   set, Airflow logs pruned, journald capped.
7. `/` is back under 60% and the monthly check is documented in a runbook.

## Risks

- **Stage 1 silently widens the existing disk alerts** from one volume to two.
  `/` is at 79%, so the warning rule may fire almost immediately after the fix.
  That is arguably correct behaviour, but it should be an expected event rather
  than a 3am surprise. Stage 3 must land with Stage 1.
- **`--path.rootfs` changes mountpoint label semantics.** Verified safe on this
  host, but the two disk alerts match on `mountpoint`, so a label regression
  would silently disable them. Check the `/` series explicitly after restart.
- **Stage 4's `du` is heavy on `/mnt/data`.** Unscheduled or run daily against
  the MinIO volume, it competes with live scraping. The weekly cadence and the
  carry-forward are the mitigation, not an optimisation.
- **Stage 5a requires a Docker daemon restart**, which restarts every container
  on the host. This is the most disruptive step in the plan and needs its own
  window and explicit confirmation.
- **Rotation is not retroactive.** Setting `max-size` does not shrink the
  existing 12.31 GiB; the truncate step is separate and easy to forget, which
  would make Stage 5a look like it failed.
- **Truncating a live container log** must use `truncate -s 0`, not `rm`. The
  daemon holds the file handle; deleting it frees no space until the container
  restarts and leaves the daemon writing to a ghost inode.
- **Loki retention deletes data.** The store currently spans 105 days, so
  enabling 90d retention drops roughly the oldest two weeks on first compaction.
  Confirm nothing depends on it first — this is irreversible.
- **Capping the buffer makes un-ingested stdout disposable.** Today
  `oauth2-proxy` and the Airflow components exist *only* as json files; once
  rotation is on, anything older than the buffer window is gone. Stage 5c must
  land with 5a, or the fix quietly destroys the auth audit trail.
- Restarting node-exporter drops no data that matters (Prometheus retains the
  series), but it is still a production container restart.

## Out of Scope

- Reducing the MinIO storage tax. That is Plan 131 (packed cold storage) and
  Plan 110 (HTML storage optimization). This plan only makes the tax *visible*.
- Moving `/var/lib/containerd` off the root disk, or resizing either volume.
  The 21 GiB of image layers is legitimate and in use; if `/` is still tight
  after Stage 5 recovers ~15 GiB, that is a separate conversation.
- Reducing Loki's own log verbosity. Flagged in Stage 5a as probably the better
  fix than rotating 8 GiB of it, but it is a config change to a service, not
  storage observability.
- Reconciling `minio_bucket_usage_object_total` (4,085,872) against the size
  histogram (3,528,807). Noted above; it affects the mean-object-size panel's
  denominator and should be settled during Stage 2, but chasing MinIO's internal
  scanner bookkeeping is not this plan's job.
