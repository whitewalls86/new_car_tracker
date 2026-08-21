# Claude Prompt: Plan 135 — Storage Observability

You are working in the `cartracker-scraper` repo. Branch off `master`.

Read `docs/plan_135_storage_observability.md` first — it is the source of truth
and records every measurement behind the decisions below. Also read the
**Stage 5 as built** section of `docs/plan_131_packed_cold_storage.md`, which
explains what this plan inherited from Plan 131 and why.

## Where this stands

**Almost nothing is built.** Two things have been applied:

| what | state |
|---|---|
| Loki's runtime log level set to `warn`, its 8.6 GB json log truncated | done in production 2026-08-14; `/` went **79% → 63%** |
| `server: log_level: warn` in `loki/loki.yml` | **committed and deployed** — survives a restart |

Everything else — Stages 1 through 6 — is outstanding.

This plan is **priority 100, row 0.1** in `docs/PLANS.md`'s closeout table, and
two other plans are waiting on it:

- **Plan 131's closeout** (row 0.3) needs Stage 1 + Stage 3. Its gate reads
  "measured, and alertable"; the inode half of that is this plan's Stage 3.
- **Plan 136** depends on this plan's monitoring conventions.

## The defect this plan starts from

`/mnt/data` — the 196 GB volume holding every Docker volume, including MinIO's
bronze bucket — **has never been monitored at all.**

`node-exporter` bind-mounts the host root at `/rootfs`
([docker-compose.yml:639](../docker-compose.yml#L639)) but never passes
`--path.rootfs=/rootfs` ([lines 640-643](../docker-compose.yml#L640-L643)). It
reads the host mount table via `--path.procfs`, correctly sees `/dev/sdb` at
`/mnt/data`, then tries to `statfs("/mnt/data")` inside its own namespace where
`/mnt` does not exist. It emits `node_filesystem_device_error` and no series.

Consequences:

- Both existing disk alerts —
  [`ct-disk-space-warning`](../grafana/provisioning/alerting/rules.yml) (uid at
  line 359) and `ct-disk-space-critical` (line 395) — match
  `node_filesystem_avail_bytes{fstype!="tmpfs",mountpoint!~"/boot.*"}`, which
  resolves to `/` alone. The volume that filled to 99-100% **twice** has never
  had capacity alerting.
- No inode alerting anywhere, on either disk. Inodes are at 61% while bytes are
  at 52% — **inodes are the binding constraint** and bind first.
- No filesystem capacity panel on the Infrastructure dashboard at all.

## Scope

Stages 1-6 of the plan doc. In short: make both disks visible, alert on them,
make "what is filling this disk" answerable from one panel, bound every
unbounded log source, and write the maintenance runbook.

**Out of scope, do not start:** reducing the MinIO storage tax (Plans 131 and
110 own that — this plan only makes it *visible*), moving `/var/lib/containerd`
off the root disk, resizing either volume, and building an archive tier for
Loki. The plan doc argues the archive case down at length under *"On archiving:
probably don't"* — the obvious target is MinIO, which lives on the same
physical volume and is the inode-constrained one, so it moves bytes sideways.

---

## Decisions already made — do not relitigate

**Inode alerting belongs here, not in Plan 131.** It was Plan 131 Stage 5's
Step 6. It moved because it is the same two rules in the same file, and because
it cannot work without Stage 1: a rule written without the node-exporter fix
evaluates `/` at 9% inodes while `/mnt/data` sits at 61% and invisible. See the
*"Inherited scope"* section in the plan doc.

**Stage 1 and Stage 3 must land together.** Stage 1 alone silently *widens* the
two existing disk alerts from one volume to two. That is arguably correct, but
it must be an expected event and not a 3am surprise. `/` is now at ~63% rather
than the 79% recorded in the plan doc, so an immediate warning fire is less
likely than the doc's Risks section assumes — **verify the current figure
before relying on that.**

**Per-run gauges and staleness are Plan 136's convention, not yours.**
Plan 136 Stage 1 is building `cartracker_metrics_last_success_timestamp_seconds`
plus NaN-on-failure for the DuckDB gauges. Do not invent a second staleness
pattern here. If Plan 136 has landed by the time you read this, adopt it.

**Physical size can never be a scrape-time metric.** A `du -sb` over the MinIO
volume ran **20+ minutes without completing**. Stage 4's job is scheduled,
never on-demand, and the weekly cadence for the MinIO volume plus the
carry-forward is the mitigation, not an optimisation.

**Never run `docker volume prune` on this host.** `/var/lib/docker/volumes` is a
symlink to `/mnt/data/docker-volumes` (Plan 105). Docker considers a volume
unused if no *running* container references it, and this host has volumes on
stopped/occasional services. A prune can delete Postgres data, the bronze
bucket, or the Loki store. There is no undo. This belongs in the Stage 6
runbook as a hard warning.

**`docker system df` hangs on this host.** It ran 5+ minutes and had to be
killed. Do not call it from Stage 4's job, and record it in the runbook.

---

## Implementation

Build in this order. Stage 1 and 3 ship together; the rest can be separate PRs.

### Step 1 — Fix node-exporter (Stage 1)

Add one flag to [docker-compose.yml:640-643](../docker-compose.yml#L640-L643):

```yaml
    command:
      - '--path.procfs=/host/proc'
      - '--path.sysfs=/host/sys'
      - '--path.rootfs=/rootfs'
      - '--collector.filesystem.mount-points-exclude=^/(sys|proc|dev|host|etc)($$|/)'
```

Verified safe already: Docker's bind mount is recursive, so `/rootfs/mnt/data`
inside the container is genuinely `/dev/sdb` — `df` there reports 195.8G and
13,107,200 inodes, matching the host.

**Verify after restart, in this order:**

1. `node_filesystem_device_error{mountpoint="/mnt/data"}` reads `0`.
2. `node_filesystem_size_bytes{mountpoint="/mnt/data"}` ≈ 2.10e+11.
3. `node_filesystem_files{mountpoint="/mnt/data"}` = 13107200.
4. **The `mountpoint="/"` series still exists and did not gain a `/rootfs`
   prefix.** node-exporter strips it, but *confirm rather than assume* — both
   disk alerts match on `mountpoint`, so a label regression silently disables
   them. This is the check that matters most.

Restarting node-exporter is a production change. **Ask before applying it.**

### Step 2 — Inode alerts (Stage 3), same PR as Step 1

In [rules.yml](../grafana/provisioning/alerting/rules.yml), group
`infra-and-data-health` (starts line 135):

- **`ct-inode-warning`** (>80%) and **`ct-inode-critical`** (>90%) on
  `(1 - node_filesystem_files_free{...} / node_filesystem_files{...}) * 100`,
  mirroring the byte rules' shape, selectors and `for:` durations exactly.
- **`ct-inode-exhaustion-forecast`** —
  `predict_linear(node_filesystem_files_free{mountpoint="/mnt/data"}[6h], 7*24*3600) < 0`,
  `for: 1h`. Fires when the current rate would exhaust inodes within a week.
  This is the rule that catches a runaway backfill while there is still time,
  and a forecast is more useful than a static threshold here because the fill
  rate is what changes when a backfill starts.

Annotations should name the mountpoint and link to the Infrastructure
dashboard, matching `ct-pipeline-failures`.

**Also confirm the existing disk alerts are still correct** once they cover two
volumes. If per-volume thresholds are wanted, split the rules.

> **`tests/test_observability_config.py` does not cover new rules
> automatically.** Its `test_rules_yml_all_uids_present` checks an **explicit
> UID allowlist**, so a new rule is invisible to it until you add the UID. The
> Plan 131 Stage 5 prompt got this wrong and the mistake is worth not
> repeating. Note also that `ct-disk-space-warning` and `ct-disk-space-critical`
> are *already* missing from that allowlist — a pre-existing gap, and you are
> the plan touching those rules, so fix it.

### Step 3 — Dashboard panels (Stage 2)

[infrastructure.json](../grafana/dashboards/infrastructure.json) currently ends
at panel id 8. Add:

1. **Filesystem Used %** — `(1 - node_filesystem_avail_bytes / node_filesystem_size_bytes)`,
   legend `{{mountpoint}}`, unit `percentunit`, thresholds 0.8 / 0.9, both
   volumes on one panel.
2. **Inode Used %** — same shape on `node_filesystem_files*`. This is the panel
   that would have shown the 61%.
3. **MinIO Logical vs Mount Physical** — `minio_bucket_usage_total_bytes`
   against `node_filesystem_size_bytes{mountpoint="/mnt/data"} - node_filesystem_avail_bytes{...}`.
   The visible divergence is the point.
4. **Storage Amplification Ratio** — the quotient, unit `none`. Today ~1.35.
5. **Mean Object Size** — `minio_bucket_usage_total_bytes / minio_bucket_usage_object_total`.
   Falling mean = rising tax; the leading indicator for whether Plan 131's
   packing is earning its keep.

**Retitle panel 6** from `MinIO Storage Used (bytes)` to **`MinIO Logical
Object Bytes`**. The current title is what made the number look like a disk
reading and is the origin of this entire plan.

**Settle the denominator discrepancy while you are here.**
`minio_bucket_usage_object_total` reports 4,085,872 while the bucket size
histogram sums to 3,528,807. Inode arithmetic favours the histogram
(3,528,807 × 2 ≈ 7.06M of 7.97M used). The two come from different scanner
bookkeeping. Panel 5's denominator depends on which you pick — **pick
deliberately and write down why**, do not let the panel silently choose.

### Step 4 — What is filling each disk (Stage 4)

A **fixed watchlist of ~20 paths**, not a filesystem explorer. Node-exporter's
textfile collector plus one scheduled job. The exact metric families and paths
are enumerated in the plan doc; follow them.

Cost discipline, which is the whole design:

- **Root disk:** `du -x --max-depth=1` is cheap (558k inodes, `-x` stops at the
  filesystem boundary). Daily.
- **`/mnt/data`:** ~8M inodes. Only `cartracker_parquet_data` is expensive.
  Walk the small volumes daily; walk the MinIO volume **weekly**, off-peak, and
  **carry the last value forward** between runs.
- Write the `.prom` file **atomically** — temp file then `rename()`.
  node-exporter reads partial files and emits garbage otherwise.

Follow this repo's job conventions: anything recurring is a **processor** — pure
function, HTTP endpoint, thin CLI — called by a thin Airflow DAG. See
`archiver/processors/` and `airflow/dags/pack_bronze_html.py`. `scripts/` is for
one-off measurement only.

### Step 5 — Bound every log source (Stage 5)

Read the plan doc's *"The log architecture: two streams, not two copies"* and
*"The principle: buffer vs. store"* before touching anything. The short version:
**Stream A (app file logs → Loki) is already correct and needs no changes.**
Everything below is Stream B.

Sub-steps 5a-5e are specified in the plan doc. Three hazards:

- **5a requires a Docker daemon restart, which restarts every container.** The
  most disruptive step in the plan. Own window, explicit confirmation.
- **5a and 5c must land together.** Capping the buffer makes un-ingested stdout
  disposable, and `oauth2-proxy` (the auth audit trail) exists *only* as a json
  file today. Ship 5a alone and the fix quietly destroys it.
- **5b deletes data.** Loki spans 105 days; enabling 90d retention drops roughly
  the oldest two weeks on first compaction. Irreversible. Confirm first.
- **Rotation is not retroactive.** `max-size` does not shrink the existing
  12.31 GiB; truncation is a separate step that is easy to forget, which would
  make 5a look like it failed. Use `truncate -s 0`, never `rm` — the daemon
  holds the handle.

### Step 6 — The maintenance runbook (Stage 6)

`docs/runbook_storage_maintenance.md`. The plan doc specifies the contents.
The point is that filesystem maintenance becomes a short, dull, scheduled task
done from a dashboard rather than an SSH session.

---

## Deploying and verifying — specifics learned the hard way

**Grafana is not port-published.** [docker-compose.yml:660-681](../docker-compose.yml#L660-L681)
has no `ports:` block; it is reachable only via Caddy at
`https://cartracker.info/grafana`. So:

- `curl http://localhost:3000/...` does not work from the host, and the
  `/api/admin/provisioning/alerting/reload` endpoint is awkward to reach behind
  oauth2-proxy.
- **`docker compose up -d` will not pick up a rules.yml or dashboard change.**
  Compose compares service *config*, not the contents of bind-mounted files, so
  Grafana does not look changed and is not recreated. The rules stay stale
  silently.
- The working deploy is `git pull` then **`docker restart cartracker-grafana`**.
  Confirm the file arrived on the VM *before* restarting.

**Loki is port-published on 3100 with `auth_enabled: false`** — that is your
validation tool for anything log-sourced.

**Prometheus-sourced rules** (everything in this plan) are validated
differently: query Prometheus for the series, and test firing by **temporarily
lowering the threshold**, never by filling the disk.

**Validate every new alert in both directions.** An alert proven only on the
positive case is not proven. Plan 131's `ct-pack-verification-refused` was
validated 2026-08-17 by showing the selector matched 81 live lines, then that
the filter rejected all 81, then that one synthetic line produced exactly 1.
The middle step is the one usually skipped and the one that matters — an empty
result means nothing unless you have shown the query is otherwise alive. See
*"The alert, validated in production"* in `plan_131_packed_cold_storage.md`.

**Production changes need explicit confirmation.** Files reach production by
`git commit` → `push` → `pull` on the VM only — never `scp`. Do not restart
containers or deploy without asking first.

**Do not declare deploy intent unnecessarily.** `POST /deploy/start` sets
`pause_long_jobs=true` by default, which stops Plan 131's packer and pruner at
their next boundary. Nothing in this plan needs it — no code change to a
running service. If a pack job is running and your deploy flow calls it anyway,
pass `{"pause_long_jobs": false}`.

**The VM is ARM64** (OCI A1.Flex); CI runners are x86_64. Any new image or
binary path must not assume the architecture.

---

## Success criteria

The plan doc lists seven. All must pass before this moves to completed:

1. `node_filesystem_device_error{mountpoint="/mnt/data"}` is `0`, and both byte
   and inode series exist for `/dev/sdb`.
2. Dashboard shows filesystem %, inode %, and logical-vs-physical for both
   volumes.
3. Inode alerts exist **and have been shown to fire** (lower the threshold
   temporarily; do not fill the disk).
4. "Why 61 vs 97?" is answerable from the dashboard without SSH.
5. "What is filling `/`?" and "what is filling `/mnt/data`?" are each answerable
   from one panel, without SSH.
6. No log source grows without bound: Docker json-file capped, Loki retention
   set, Airflow logs pruned, journald capped.
7. `/` is back under 60% and the monthly check is documented in a runbook.

---

## What would make this plan wrong

- **Shipping Stage 1 without Stage 3.** It widens two existing alerts to a
  second volume with no warning.
- **Writing inode alerts without the node-exporter fix.** They evaluate `/` at
  9% and look healthy forever. This is exactly why the work moved out of Plan
  131.
- **Assuming the `mountpoint="/"` label survives `--path.rootfs`.** Check it.
  A regression silently disables both disk alerts.
- **Running Stage 4's `du` daily against the MinIO volume**, or on demand. It
  competes with live scraping and does not complete.
- **Shipping 5a without 5c** — destroys the `oauth2-proxy` auth audit trail.
- **Enabling Loki retention without confirming** the oldest two weeks are
  expendable. Irreversible.
- **Forgetting that rotation is not retroactive**, then concluding 5a failed.
- **Building a second staleness convention** instead of adopting Plan 136's.
- **Adding a `.prom` file non-atomically.** node-exporter will emit garbage.
- **Treating `docker system df` as a diagnostic here.** It hangs.
- **Reaching for `docker volume prune`.** It can delete the bronze bucket.
