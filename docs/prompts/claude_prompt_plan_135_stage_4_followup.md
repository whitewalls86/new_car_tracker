# Claude Prompt: Plan 135 follow-up (Stage 4 defect, then Stages 5-6)

You are working in the `cartracker-scraper` repo. Branch off `master`.

Read `docs/plan_135_storage_observability.md` first — it is the source of truth.
The sections that matter most for this session are **"Stage 4's first production
run (2026-08-18)"** and **"Stage 4 as built"**. Also read **"D3b validated in
production"** in `docs/plan_131_packed_cold_storage.md`, which records how to
deploy against a running long job without causing collateral DAG failures.

## Where this stands

**Stages 1-4 are live.** Deployed and verified 2026-08-17/18 (PRs #204, #205).

| Success criterion | State |
|---|---|
| 1. `/mnt/data` byte + inode series exist, `device_error` = 0 | ✅ met |
| 2. Dashboard shows filesystem %, inode %, logical-vs-physical | ✅ met |
| 3. Inode alerts exist **and fire** (Telegram confirmed) | ✅ met |
| 4. "Why 61 vs 97?" answerable without SSH | ✅ met |
| 5. "What is filling each disk?" from one panel | ✅ met for `/` + daily volumes; MinIO pending its first weekly walk |
| 6. No log source grows without bound | ❌ **Stage 5, not started** |
| 7. `/` under 60%, monthly check in a runbook | ❌ `/` is at ~64%; **Stage 6 not started** |

`/` was 64% and `/mnt/data` 41% bytes / 30% inodes as of 2026-08-18.

---

## Task 1 — Fix the Stage 4 tier defect (do this first, it is small)

**The bug:** `cartracker_airflow_logs` is in `DAILY_VOLUMES` in
`archiver/processors/disk_usage.py`. It was classified by *size* (2.9 GiB,
obviously small) when `du` cost scales with **inodes** — and it holds ~1.2M of
them, roughly twice the entire root filesystem. Measured on the first
production run, it was **397s of a 456s walk — 87%**.

Fix:

- Move `cartracker_airflow_logs` from `DAILY_VOLUMES` to the weekly tier
  (currently `WEEKLY_VOLUMES`). Expected effect: daily walk **456s → ~59s**.
- The gating flag is then no longer MinIO-specific: rename `include_minio` →
  `include_slow` through the processor, the `/disk-usage/run` endpoint, the
  `disk_usage` DAG (`should_include_minio` → `should_include_slow`), and the
  tests. Nothing outside the repo depends on the name.
- Rename `WEEKLY_VOLUMES` to something that states the criterion, and **add a
  test asserting tier membership is decided by inode count, not volume size** —
  that is the assumption that failed, so it is the one worth pinning.
- Update the `/mnt/data by Docker Volume` panel description in
  `grafana/dashboards/infrastructure.json`: it currently says only
  `cartracker_parquet_data` is walked weekly.

**Do not** "fix" this by making the daily job faster in general. `du` is already
the C implementation and the cost is inherently O(inodes). Sample-and-multiply
is right for one-off *probing* and wrong for a monitoring gauge — it produces an
estimate that drifts, and you cannot separate real growth from sampling noise.
The real fix for the inodes themselves is Stage 5d, below.

## Task 2 — Correct the deploy instruction

PR #205's description says the deploy is `docker compose up -d`. **That is
wrong and it matters.** `archiver/Dockerfile` does `COPY . .`, so processor and
app code is baked into the image; `archiver` and `pack-worker` share the
`cartracker-archiver` tag. A bare `up -d` restarts pack-worker on the *old*
image and `/disk-usage/run` 404s while looking deployed.

The correct sequence, which belongs in the Stage 6 runbook:

```bash
git pull
docker compose build archiver                     # shared tag; covers pack-worker
docker compose up -d node-exporter pack-worker archiver
docker exec cartracker-pack-worker ls /app/archiver/processors/disk_usage.py
docker inspect cartracker-node-exporter --format '{{json .Args}}'
```

**Verify what loaded, never container uptime.** This bit twice in one evening:
`docker restart` does not apply a compose `command:` change (it reuses the
existing container's config) while `docker compose up -d` does not pick up a
changed *bind-mounted file* (compose sees no config drift). They are exact
opposites. See `docs/plan_135_storage_observability.md`, "Stages 1-3 shipped and
validated".

## Task 3 — Stage 5, the disruptive one

Read the plan doc's *"The log architecture: two streams, not two copies"* and
*"The principle: buffer vs. store"* first. **Stream A (app file logs → Loki) is
already correct and needs no changes.** Everything in Stage 5 is Stream B.

Hazards, unchanged:

- **5a restarts the Docker daemon and therefore every container.** Own window,
  explicit confirmation.
- **5a and 5c must land together.** Capping the buffer makes un-ingested stdout
  disposable, and `oauth2-proxy` — the auth audit trail — exists *only* as a
  json file today.
- **5b deletes data.** Loki spans >90 days; enabling 90d retention drops the
  oldest on first compaction. Irreversible. Confirm first.
- **Rotation is not retroactive.** `max-size` does not shrink existing files.
  Truncate with `truncate -s 0`, never `rm` — the daemon holds the handle.

**Updated numbers for 5d.** The plan estimates ~2.9 GiB reclaimed from Airflow
task logs. The Stage 4 walk measured the volume at **6.33 GiB physical** — the
gap is the small-file tax (~1.2M files × 4 KiB block rounding ≈ 4.9 GiB floor),
not growth. So 5d is worth **~6.3 GiB and ~1.2M inodes**, the largest inode win
on `/mnt/data` outside MinIO itself, and it also retires Task 1's tier problem
at source: once the logs are pruned to 30 days, `cartracker_airflow_logs` can
move back to the daily tier legitimately.

Note `/var/lib/docker/containers` measured **5.42 GiB**, down from the 12.31 GiB
in the plan doc — the Loki truncate on 08-14 accounts for the difference. Re-read
the current value before sizing 5a rather than quoting the doc.

## Task 4 — Stage 6, the runbook

`docs/runbook_storage_maintenance.md`. Contents are specified in the plan doc.
Fold in, because they were all learned the hard way and currently live only in
plan docs:

- the build-then-recreate deploy sequence from Task 2, and the
  restart-vs-recreate distinction
- **never `docker volume prune`** — `/var/lib/docker/volumes` symlinks to
  `/mnt/data/docker-volumes` (Plan 105); it can delete Postgres, the bronze
  bucket, or the Loki store, with no undo
- **`docker system df` hangs on this host** — 5+ minutes, had to be killed
- deploy intent: **build before declaring it**, keep the window under 10 minutes
  or every DAG's `deploy_intent_sensor` (`timeout=600`) fails its first task
- fire-testing an alert: hold the test rule **past `group_wait` (30s)** or it
  cancels its own notification

---

## Decisions already made — do not relitigate

- **`minio_bucket_usage_object_total` is the mean-object-size denominator.**
  MinIO emits two overlapping bucket schemes in one metric; summing the size
  distribution double-counts the 1 KB-1 MB range. Settled with measurements in
  the plan doc.
- **One `.prom` file with carry-forward, not two files.** Splitting a metric
  family across files makes node-exporter rewrite the HELP text and reject one
  ([node_exporter#1885](https://github.com/prometheus/node_exporter/issues/1885)).
- **`du -s -x --block-size=1`, not `-b`.** Physical, not apparent. The whole
  point is the per-object floor, which apparent size hides.
- **Per-path read-only mounts on pack-worker, not `/:/rootfs:ro`.** The
  watchlist is fixed by design; adding a path should be a deliberate compose
  edit.
- **Staleness is Plan 136's convention.** `cartracker_disk_usage_measured_timestamp_seconds`
  is per-series only because carry-forward means two series in one file can be a
  week apart. Fold it into Plan 136's pattern if that lands and subsumes it.

## What would make this session wrong

- Making the daily walk "faster" instead of moving the expensive volume to the
  weekly tier.
- Shipping 5a without 5c — destroys the `oauth2-proxy` auth audit trail.
- Enabling Loki retention without confirming the oldest data is expendable.
- Declaring deploy intent and then doing a slow build inside the window.
- Trusting `docker restart`, or container uptime, as evidence a deploy landed.
- Quoting the plan doc's 2026-08-14 figures as current. Plan 131's packer and
  the Loki truncate moved several of them; re-measure before relying on one.

## Related, not in scope

`docs/plan_139_test_suite_maintenance.md` is a skeleton from the same evening.
Its one time-sensitive item is `ops/metrics/duckdb_gauges.py` at **25%
coverage** — Plan 136 Stage 1 builds its staleness convention on that module.
Cover it before Plan 136 starts, not as part of this session.
